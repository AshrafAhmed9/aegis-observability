import asyncio
import random
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class FaultInjector:
    name: str = "base"
    target_service: str = ""
    cascade_service: str = ""
    ramp_seconds: float = 0.0
    failure_seconds: float = 0.0
    recovery_seconds: float = 0.0
    # Real cascades lag the root cause (retry/timeout propagation delay).
    # Without this, the cascade and target events land in the same tick and
    # the upstream (symptom) span's earlier index gives it an earlier
    # timestamp than the true root cause, inverting the EVENT_TIME tie-break.
    cascade_delay_seconds: float = 5.0

    def phase(self, elapsed: float) -> str:
        if elapsed < self.ramp_seconds:
            return "ramp"
        if elapsed < self.ramp_seconds + self.failure_seconds:
            return "failure"
        if elapsed < self.ramp_seconds + self.failure_seconds + self.recovery_seconds:
            return "recovery"
        return "done"

    def is_finished(self, elapsed: float) -> bool:
        return self.phase(elapsed) == "done"

    def apply_target(self, elapsed: float, rng: random.Random) -> dict:
        raise NotImplementedError

    def apply_cascade(self, elapsed: float, rng: random.Random) -> dict:
        raise NotImplementedError


class RedisConnectionLeakFault(FaultInjector):
    """Predictable: connection_pool_usage climbs steadily toward 1.0."""
    name = "redis_connection_leak"
    target_service = "redis-cache"
    cascade_service = "api-gateway"
    ramp_seconds = 240.0
    failure_seconds = 20.0
    recovery_seconds = 40.0

    def apply_target(self, elapsed, rng):
        phase = self.phase(elapsed)
        if phase == "ramp":
            frac = min(1.0, elapsed / self.ramp_seconds)
            pool = min(0.98, 0.4 + frac * 0.55 + rng.uniform(-0.015, 0.015))
            return {"connection_pool_usage": round(pool, 3), "active_connections": int(pool * 200)}
        if phase == "failure":
            return {
                "connection_pool_usage": 1.0, "active_connections": 200,
                "severity": "ERROR", "err_class": "redis.exceptions.ConnectionError",
                "msg": "Connection pool exhausted. Timeout waiting for connection (5000ms)",
            }
        if phase == "recovery":
            rec_elapsed = elapsed - self.ramp_seconds - self.failure_seconds
            frac = min(1.0, rec_elapsed / self.recovery_seconds)
            pool = max(0.4, 1.0 - frac * 0.6)
            out = {"connection_pool_usage": round(pool, 3), "active_connections": int(pool * 200)}
            if pool > 0.7:
                out["severity"] = "WARNING"
            return out
        return {"connection_pool_usage": 0.4, "active_connections": 80}

    def apply_cascade(self, elapsed, rng):
        return {
            "status_code": 504, "latency_ms": round(rng.uniform(12000, 18000), 1),
            "severity": "CRITICAL", "err_class": "GatewayTimeout",
            "msg": "Upstream timeout on checkout handler cascade",
        }


class QueueBacklogFault(FaultInjector):
    """Predictable: payment-worker queue_depth climbs steadily toward 500."""
    name = "queue_backlog"
    target_service = "payment-worker"
    cascade_service = "api-gateway"
    ramp_seconds = 200.0
    failure_seconds = 20.0
    recovery_seconds = 40.0

    def apply_target(self, elapsed, rng):
        phase = self.phase(elapsed)
        if phase == "ramp":
            frac = min(1.0, elapsed / self.ramp_seconds)
            depth = 15 + frac * 545 + rng.uniform(-10, 10)
            return {"queue_depth": max(0, int(depth))}
        if phase == "failure":
            return {
                "queue_depth": rng.randint(560, 650),
                "severity": "ERROR", "err_class": "celery.exceptions.WorkerLostError",
                "msg": "Worker thread pool starvation. Task queue backlog exceeded capacity.",
            }
        if phase == "recovery":
            rec_elapsed = elapsed - self.ramp_seconds - self.failure_seconds
            frac = min(1.0, rec_elapsed / self.recovery_seconds)
            depth = max(20, 560 - frac * 540)
            out = {"queue_depth": int(depth)}
            if depth > 200:
                out["severity"] = "WARNING"
            return out
        return {"queue_depth": 20}

    def apply_cascade(self, elapsed, rng):
        return {
            "status_code": 504, "latency_ms": round(rng.uniform(10000, 16000), 1),
            "severity": "CRITICAL", "err_class": "GatewayTimeout",
            "msg": "Upstream timeout on checkout handler cascade",
        }


class DeadlockBurstFault(FaultInjector):
    """Deliberately unpredictable: no ramp, sudden deadlock. Trend detector
    can't see it coming — only ANOMALY/ERROR_RATE and RCA catch it."""
    name = "deadlock_burst"
    target_service = "postgres-db"
    cascade_service = "checkout-service"
    ramp_seconds = 0.0
    failure_seconds = 15.0
    recovery_seconds = 20.0

    def apply_target(self, elapsed, rng):
        phase = self.phase(elapsed)
        if phase == "failure":
            return {
                "lock_duration_ms": round(rng.uniform(3000, 6000), 1),
                "latency_ms": round(rng.uniform(4000, 6000), 1),
                "severity": "CRITICAL", "err_class": "postgres.deadlock",
                "msg": "PostgreSQL deadlock detected: circular lock wait between concurrent transactions.",
            }
        if phase == "recovery":
            rec_elapsed = elapsed - self.failure_seconds
            frac = min(1.0, rec_elapsed / self.recovery_seconds)
            latency = max(20, 4000 * (1 - frac))
            out = {"latency_ms": round(latency, 1)}
            if latency > 500:
                out["severity"] = "WARNING"
            return out
        return {}

    def apply_cascade(self, elapsed, rng):
        return {
            "severity": "ERROR", "err_class": "sqlalchemy.exc.OperationalError",
            "msg": "Database transaction deadlock: transaction aborted by PostgreSQL engine.",
        }


FAULTS = {
    RedisConnectionLeakFault.name: RedisConnectionLeakFault,
    QueueBacklogFault.name: QueueBacklogFault,
    DeadlockBurstFault.name: DeadlockBurstFault,
}


class SimulatedFleet:
    TICK_SECONDS = 1.5
    DOWNSTREAM_SERVICES = ("redis-cache", "postgres-db", "payment-worker")

    def __init__(self, emit: Callable[[dict], None], seed: Optional[int] = None):
        self._emit = emit
        self._rng = random.Random(seed)
        self._trace_counter = 0
        self._fault: Optional[FaultInjector] = None
        self._fault_started_at: Optional[float] = None
        self._task: Optional[asyncio.Task] = None
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            self._task = None
        self._fault = None
        self._fault_started_at = None

    def inject(self, fault_name: str) -> dict:
        fault_cls = FAULTS.get(fault_name)
        if fault_cls is None:
            raise ValueError(f"Unknown fault '{fault_name}'. Available: {sorted(FAULTS)}")
        self._fault = fault_cls()
        self._fault_started_at = time.monotonic()
        return self.status()

    def status(self) -> dict:
        elapsed = self._fault_elapsed()
        return {
            "running": self._running,
            "fault": self._fault.name if self._fault else None,
            "phase": self._fault.phase(elapsed) if (self._fault and elapsed is not None) else None,
            "elapsed_seconds": round(elapsed, 1) if elapsed is not None else None,
        }

    def _fault_elapsed(self) -> Optional[float]:
        if self._fault_started_at is None:
            return None
        return time.monotonic() - self._fault_started_at

    async def _run_loop(self) -> None:
        try:
            while self._running:
                self.tick()
                await asyncio.sleep(self.TICK_SECONDS)
        except asyncio.CancelledError:
            pass

    def tick(self) -> None:
        n_traces = self._rng.randint(1, 3)
        for _ in range(n_traces):
            self._emit_trace()
        elapsed = self._fault_elapsed()
        if self._fault is not None and elapsed is not None and self._fault.is_finished(elapsed):
            self._fault = None
            self._fault_started_at = None

    def _cascade_active(self, elapsed: Optional[float]) -> bool:
        if self._fault is None or elapsed is None:
            return False
        if self._fault.phase(elapsed) != "failure":
            return False
        return elapsed >= self._fault.ramp_seconds + self._fault.cascade_delay_seconds

    def _emit_trace(self) -> None:
        self._trace_counter += 1
        trace_id = f"sim-{self._trace_counter:06d}"
        base_time = datetime.now(timezone.utc)
        elapsed = self._fault_elapsed()
        phase = self._fault.phase(elapsed) if (self._fault and elapsed is not None) else None

        events = []
        cascade_now = self._cascade_active(elapsed)

        gw_span = f"{trace_id}-s1"
        gw_fields = self._baseline("api-gateway")
        if cascade_now and self._fault.cascade_service == "api-gateway":
            gw_fields.update(self._fault.apply_cascade(elapsed, self._rng))
        events.append(self._make_event(trace_id, gw_span, None, "api-gateway", base_time, 0, gw_fields))

        co_span = f"{trace_id}-s2"
        co_fields = self._baseline("checkout-service")
        if cascade_now and self._fault.cascade_service == "checkout-service":
            co_fields.update(self._fault.apply_cascade(elapsed, self._rng))
        events.append(self._make_event(trace_id, co_span, gw_span, "checkout-service", base_time, 1, co_fields))

        downstream = set(self._rng.sample(self.DOWNSTREAM_SERVICES, k=self._rng.randint(1, 2)))
        if self._fault is not None:
            downstream.add(self._fault.target_service)

        for i, svc in enumerate(sorted(downstream), start=2):
            span_id = f"{trace_id}-s{i + 1}"
            fields = self._baseline(svc)
            if self._fault is not None and svc == self._fault.target_service:
                fields.update(self._fault.apply_target(elapsed, self._rng))
            events.append(self._make_event(trace_id, span_id, co_span, svc, base_time, i, fields))

        for event in events:
            self._emit(event)

    def _baseline(self, service: str) -> dict:
        rng = self._rng
        if service == "api-gateway":
            return {"latency_ms": round(rng.uniform(35, 90), 1), "status_code": 200,
                    "method": "POST", "path": "/api/v1/checkout"}
        if service == "checkout-service":
            return {"latency_ms": round(rng.uniform(25, 70), 1)}
        if service == "redis-cache":
            pool = rng.uniform(0.35, 0.45)
            return {"connection_pool_usage": round(pool, 3), "active_connections": int(pool * 200)}
        if service == "postgres-db":
            return {"lock_duration_ms": round(rng.uniform(15, 60), 1),
                    "query": "SELECT * FROM inventory WHERE item_id = ?", "status": "SUCCESS"}
        if service == "payment-worker":
            return {"queue_depth": rng.randint(8, 30), "task_name": "tasks.process_payment"}
        return {}

    def _make_event(self, trace_id, span_id, parent_span_id, service, base_time, offset_index, fields) -> dict:
        ts = base_time + timedelta(milliseconds=offset_index * 35)
        event = {
            "trace_id": trace_id,
            "span_id": span_id,
            "service": service,
            "timestamp": _iso(ts),
        }
        if parent_span_id:
            event["parent_span_id"] = parent_span_id
        event.update(fields)
        return event
