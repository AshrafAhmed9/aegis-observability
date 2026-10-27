"""
Makes up fake telemetry for four services (api, cache, queue, database).

Each "episode" is one outage where we choose which service breaks, so the
right answer is known. That lets us (a) feed demo traffic to the API,
(b) train the ML model, and (c) grade the root-cause engine (run_eval.py).

Timestamps are plain seconds counting up from 0, not real clock time, so
tests and evaluation run instantly.
"""

import random

SERVICES = ["api", "cache", "queue", "database"]

# Each fault names the service that breaks first (the "target"), the error it
# logs, and the error that the api logs afterwards as a result.
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

# A struggling service logs one WARNING, then retries with longer and longer
# waits before it finally gives up and logs the ERROR. That gap is the
# warning time the ML model has to work with. We just measure whatever lead
# time comes out (run_eval.py); we don't aim for a number.
RETRY_BACKOFF_STEPS_SECONDS = [15, 30, 60, 60]  # seconds waited before each retry
BACKOFF_JITTER_SECONDS = 15  # up to this much random extra wait per retry


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
    """Hands out span ids (span-1, span-2, ...) that are unique within one episode."""

    def __init__(self):
        self.count = 0

    def next(self):
        self.count += 1
        return f"span-{self.count}"


def _emit_baseline_traffic(events, spans, trace_id, start_time):
    # Three rounds of normal INFO events from every service, 5s apart.
    t = start_time
    for _ in range(3):
        for service in SERVICES:
            events.append(make_event(trace_id, spans.next(), None, service, t, "INFO"))
        t += 5.0
    return t


def _emit_target_breaking(events, spans, trace_id, start_time, fault, rng):
    # The target logs a WARNING first. That is the earliest sign of trouble.
    # It then retries with growing waits before finally logging the ERROR.
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
    # The api fails because the target failed. parent_span_id records that
    # link, which gives rca.py its cause -> effect arrow.
    t = start_time
    for _ in range(4):
        t += 5.0
        events.append(
            make_event(trace_id, spans.next(), parent_span_id, "api", t, "CRITICAL", fault["cascade_error"])
        )
    return t


def generate_episode(seed, fault_name):
    """Builds one outage. Returns (events, root_cause_service, warning_time,
    failure_time). The last three are the known right answers, used for
    grading. The same seed always gives the same episode."""
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
    """Builds episodes_per_fault outages of each fault type. The default of 26
    gives 3 x 26 = 78 episodes, the set run_eval.py grades."""
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
