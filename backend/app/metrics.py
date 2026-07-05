from prometheus_client import Counter, Gauge, Histogram

EVENTS_INGESTED = Counter("aegis_events_ingested_total", "Total events ingested")
TRACES_EMITTED = Counter("aegis_traces_emitted_total", "Total traces emitted (closed)")
INCIDENTS_PROCESSED = Counter("aegis_incidents_processed_total", "Total incidents processed")
LATE_EVENTS = Counter("aegis_late_events_total", "Total late events received")
OPEN_TRACES = Gauge("aegis_open_traces", "Currently open trace buffers")
CORRELATION_DURATION = Histogram(
    "aegis_correlation_duration_seconds",
    "Time to process an incident through the full pipeline",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
ROOT_CAUSE_CLASS = Counter(
    "aegis_root_cause_class_total",
    "Root causes by class",
    ["root_cause_class"],
)
PREDICTIONS_ACTIVE = Gauge("aegis_predictions_active", "Currently active failure predictions")
PREDICTED_BREACH_ETA = Gauge(
    "aegis_predicted_breach_eta_seconds",
    "Seconds until predicted metric threshold breach",
    ["service", "metric"],
)
PREDICTIONS_EMITTED = Counter(
    "aegis_predictions_emitted_total",
    "Total predictions emitted by kind",
    ["kind"],
)
