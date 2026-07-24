"""
Synthetic telemetry generator for a small service fleet (api, cache, queue,
database). Produces fault episodes with a known root cause, used both for
live demo traffic and for grading the correlation engine (see eval/run_eval.py).

Timestamps are plain seconds counting up from 0, not wall-clock time -- enough
to exercise event-time watermarks without real delays.
"""

import random

SERVICES = ["api", "cache", "queue", "database"]

# Each fault names the service that fails first (the "target") and the error
# it logs, plus the downstream error the api service logs as a result.
FAULT_TYPES = {
    "redis_leak": {
        "target": "cache",
        "target_error": "redis.exceptions.ConnectionError",
        "cascade_error": "GatewayTimeout",
    },
    "queue_backlog": {
        "target": "queue",
        "target_error": "celery.exceptions.WorkerLostError",
        "cascade_error": "GatewayTimeout",
    },
    "deadlock_burst": {
        "target": "database",
        "target_error": "postgres.deadlock",
        "cascade_error": "sqlalchemy.exc.OperationalError",
    },
}

# A struggling service logs one WARNING, then keeps retrying with backoff
# before it finally gives up and logs the ERROR. This mirrors a real
# reconnection-storm: a handful of retries at growing intervals. The total
# backoff time (plus some jitter) is the gap ml.py's model has to predict
# across -- whatever lead time that produces gets measured honestly by
# eval/run_eval.py, not targeted in advance.
RETRY_BACKOFF_STEPS_SECONDS = [15, 30, 60, 60]  # 4 retries: 15s, 30s, 60s, 60s apart
BACKOFF_JITTER_SECONDS = 15


def make_event(trace_id, span_id, parent_span_id, service, timestamp, severity, error_class=None):
    return {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "service": service,
        "timestamp": timestamp,
        "severity": severity,
        "error_class": error_class,
    }


class SpanCounter:
    """Issues unique span ids within one episode."""

    def __init__(self):
        self.count = 0

    def next(self):
        self.count += 1
        return f"span-{self.count}"


def _emit_baseline_traffic(events, spans, trace_id, start_time):
    # Healthy traffic before the fault, so an episode isn't errors-only.
    t = start_time
    for _ in range(3):
        for service in SERVICES:
            events.append(make_event(trace_id, spans.next(), None, service, t, "INFO"))
        t += 5.0
    return t


def _emit_target_breaking(events, spans, trace_id, start_time, fault, rng):
    # First WARNING fires immediately -- that's the earliest signal a
    # predictor could act on. The service then retries with backoff
    # (RETRY_BACKOFF_STEPS_SECONDS) before finally logging the ERROR.
    warning_time = start_time + 15.0
    warning_span = spans.next()
    events.append(make_event(trace_id, warning_span, None, fault["target"], warning_time, "WARNING"))

    t = warning_time
    for step in RETRY_BACKOFF_STEPS_SECONDS:
        t += step + rng.uniform(0, BACKOFF_JITTER_SECONDS)

    error_time = t
    error_span = spans.next()
    events.append(make_event(trace_id, error_span, None, fault["target"], error_time, "ERROR", fault["target_error"]))

    return error_time, error_span, warning_time


def _emit_cascade_failure(events, spans, trace_id, start_time, parent_span_id, fault):
    # parent_span_id ties these events to the target's failure, giving rca.py
    # a real cause -> effect edge rather than two unrelated failures.
    t = start_time
    for _ in range(4):
        t += 5.0
        events.append(
            make_event(trace_id, spans.next(), parent_span_id, "api", t, "CRITICAL", fault["cascade_error"])
        )
    return t


def generate_episode(seed, fault_name):
    """Returns (events, root_cause_service, warning_time, failure_time).
    root_cause_service is the ground truth for RCA grading; warning_time and
    failure_time are the ground truth for lead-time measurement."""
    rng = random.Random(seed)
    fault = FAULT_TYPES[fault_name]
    trace_id = f"{fault_name}-{seed}"
    spans = SpanCounter()
    events = []

    t = _emit_baseline_traffic(events, spans, trace_id, start_time=rng.uniform(0, 5))
    failure_time, target_error_span, warning_time = _emit_target_breaking(events, spans, trace_id, t, fault, rng)
    _emit_cascade_failure(events, spans, trace_id, failure_time, target_error_span, fault)

    return events, fault["target"], warning_time, failure_time


def generate_episodes(episodes_per_fault=26):
    """episodes_per_fault=26 across 3 fault types yields 78 labeled episodes --
    the exact set eval/run_eval.py scores the correlation engine against."""
    episodes = []
    seed = 0
    for fault_name in FAULT_TYPES:
        for _ in range(episodes_per_fault):
            seed += 1
            events, root_cause_service, warning_time, failure_time = generate_episode(seed, fault_name)
            episodes.append({
                "seed": seed,
                "fault_name": fault_name,
                "events": events,
                "root_cause_service": root_cause_service,
                "warning_time": warning_time,
                "failure_time": failure_time,
            })
    return episodes
