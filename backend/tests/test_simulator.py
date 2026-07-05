import asyncio
import random
import time
from datetime import datetime, timezone, timedelta

import app.simulator as sim_mod
from app.simulator import SimulatedFleet, FAULTS


def test_baseline_traces_have_valid_span_chains():
    events = []
    fleet = SimulatedFleet(emit=events.append, seed=42)
    fleet.tick()
    assert events
    by_trace = {}
    for ev in events:
        by_trace.setdefault(ev["trace_id"], []).append(ev)
    for trace_id, trace_events in by_trace.items():
        span_ids = {e["span_id"] for e in trace_events}
        roots = [e for e in trace_events if "parent_span_id" not in e]
        assert len(roots) == 1
        for e in trace_events:
            if "parent_span_id" in e:
                assert e["parent_span_id"] in span_ids


def test_all_faults_registered_and_ramp_monotonically():
    for name, fault_cls in FAULTS.items():
        fault = fault_cls()
        assert fault.name == name
        if fault.target_service and fault.ramp_seconds > 0:
            rng = random.Random(1)
            values = []
            for t in [0, fault.ramp_seconds * 0.25, fault.ramp_seconds * 0.5,
                      fault.ramp_seconds * 0.75, fault.ramp_seconds * 0.99]:
                out = fault.apply_target(t, rng)
                metric_key = next((k for k in out if k in
                                    ("connection_pool_usage", "queue_depth")), None)
                if metric_key:
                    values.append(out[metric_key])
            assert values == sorted(values)


def test_inject_unknown_fault_raises():
    fleet = SimulatedFleet(emit=lambda e: None)
    try:
        fleet.inject("not_a_real_fault")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_seeded_runs_are_reproducible():
    events_a, events_b = [], []
    fleet_a = SimulatedFleet(emit=events_a.append, seed=7)
    fleet_b = SimulatedFleet(emit=events_b.append, seed=7)
    for _ in range(3):
        fleet_a.tick()
        fleet_b.tick()

    def _strip_ts(events):
        return [{k: v for k, v in e.items() if k != "timestamp"} for e in events]

    assert _strip_ts(events_a) == _strip_ts(events_b)


def test_cascade_lags_target_so_root_cause_ordering_is_correct(monkeypatch):
    """Regression: cascade (symptom) must not out-race the target (root
    cause) in timestamp order, or the EVENT_TIME RCA fallback picks the
    wrong service as root cause.

    Real ticks are 1.5 real seconds apart, which dwarfs the 0-105ms
    intra-trace span offsets. A naive test that fast-forwards
    `_fault_started_at` without advancing wall time doesn't preserve that
    gap and can invert the ordering by pure coincidence, so the wall clock
    itself is faked here to advance one tick's worth of real time per call.
    """
    fake_now = [datetime(2026, 1, 1, tzinfo=timezone.utc)]

    class FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fake_now[0]

    monkeypatch.setattr(sim_mod, "datetime", FakeDateTime)

    events = []
    fleet = SimulatedFleet(emit=events.append, seed=3)
    fleet.inject("redis_connection_leak")
    fault = fleet._fault

    # Stage 1: just past ramp -> failure, well before cascade_delay elapses.
    # Only the target (redis-cache) should error here.
    fleet._fault_started_at = time.monotonic() - (fault.ramp_seconds + 0.5)
    for _ in range(3):
        fleet.tick()
        fake_now[0] += timedelta(seconds=fleet.TICK_SECONDS)

    # Stage 2: now past cascade_delay too — the cascade (api-gateway) fires.
    fleet._fault_started_at = time.monotonic() - (fault.ramp_seconds + fault.cascade_delay_seconds + 2.0)
    for _ in range(3):
        fleet.tick()
        fake_now[0] += timedelta(seconds=fleet.TICK_SECONDS)

    def first_error_ts(service):
        matches = [e["timestamp"] for e in events
                   if e["service"] == service and e.get("severity") in ("ERROR", "CRITICAL")]
        return min(matches) if matches else None

    redis_ts = first_error_ts("redis-cache")
    gw_ts = first_error_ts("api-gateway")
    assert redis_ts is not None
    assert gw_ts is not None
    assert redis_ts < gw_ts


def test_start_stop_lifecycle():
    fleet = SimulatedFleet(emit=lambda e: None)
    assert not fleet.running

    async def _run():
        fleet.start()
        assert fleet.running
        await asyncio.sleep(0)
        fleet.stop()
        assert not fleet.running

    asyncio.run(_run())
