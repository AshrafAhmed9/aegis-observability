"""
The machine-learning part. A gradient-boosted model watches for early signs
that a service is about to fail.

It runs in "shadow mode": its predictions are recorded and checked against
what really happens, but it never decides the root cause. That is rca.py's job.

Also here: drift detection. If live traffic starts to look different from the
data the model was trained on, the model may no longer be trustworthy.
"""

import json
import math

import joblib

FEATURE_NAMES = ["warning_count", "error_count", "seconds_since_last_event", "degraded_services_count"]

# The model is only trained on these three services. "api" only ever shows up
# as a victim of someone else's failure, never as an example of its own, so
# any score for "api" would be a guess about something the model never saw.
TRAINED_SERVICES = {"cache", "queue", "database"}

# A service counts as "high risk" when the model's score is at least this.
RISK_THRESHOLD = 0.5

# Drift levels. PSI (Population Stability Index) is a number for how much two
# distributions differ: below 0.1 is fine, above 0.25 is a big change. These
# are the usual cut-offs analysts use.
PSI_OK_MAX = 0.1
PSI_WARN_MAX = 0.25


def extract_features(prior_events, current_event):
    """Turns one event into 4 numbers the model can read, using only events
    from before it. A live system can't see the future, so training must not
    either."""
    service = current_event["service"]
    same_service_before = [e for e in prior_events if e["service"] == service]

    warning_count = sum(1 for e in same_service_before if e["severity"] in ("WARNING", "ERROR", "CRITICAL"))
    error_count = sum(1 for e in same_service_before if e["severity"] in ("ERROR", "CRITICAL"))

    if same_service_before:
        seconds_since_last_event = current_event["timestamp"] - same_service_before[-1]["timestamp"]
    else:
        seconds_since_last_event = 0.0

    degraded_services = {e["service"] for e in prior_events if e["severity"] in ("ERROR", "CRITICAL")}

    return [warning_count, error_count, seconds_since_last_event, len(degraded_services)]


def build_training_rows(episode):
    """Turns one episode into training examples of (numbers, label).

    Label 1: the failing service's WARNING event. This is the moment the model
    should raise an alarm.
    Label 0: normal INFO events from services that stay healthy. These are
    examples of "nothing is wrong here".
    """
    events = sorted(episode["events"], key=lambda e: e["timestamp"])
    target_service = episode["root_cause_service"]
    non_failing_services = TRAINED_SERVICES - {target_service}

    rows = []
    seen = []
    for event in events:
        if event["service"] == target_service and event["severity"] == "WARNING":
            features = extract_features(seen, event)
            rows.append((features, 1))
        elif event["service"] in non_failing_services and event["severity"] == "INFO":
            features = extract_features(seen, event)
            rows.append((features, 0))
        seen.append(event)

    return rows


def compute_feature_distribution(feature_rows):
    """Records the spread of each feature in the training data as three
    markers: p10, p50, p90 (the values 10%, 50% and 90% of the way up). Drift
    checks compare live data against these."""
    distribution = {}
    for index, name in enumerate(FEATURE_NAMES):
        values = sorted(row[index] for row in feature_rows)
        distribution[name] = {
            "p10": _percentile(values, 0.10),
            "p50": _percentile(values, 0.50),
            "p90": _percentile(values, 0.90),
        }
    return distribution


def _percentile(sorted_values, fraction):
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int(fraction * len(sorted_values)))
    return sorted_values[index]


class DriftMonitor:
    """Checks whether live data still looks like the training data.

    For each feature, the training p10/p50/p90 split the number line into 4
    buckets. In training, about 10%/40%/40%/10% of values fall in them. We
    count where live values fall and compare. A big mismatch (a high PSI
    score) means the world has changed since the model was trained."""

    EXPECTED_BUCKET_SHARE = [0.10, 0.40, 0.40, 0.10]
    WINDOW_SIZE = 200  # only the most recent values count

    def __init__(self, training_distribution):
        self.training_distribution = training_distribution
        self.observed_values = {name: [] for name in FEATURE_NAMES}

    def observe(self, features):
        for name, value in zip(FEATURE_NAMES, features):
            values = self.observed_values[name]
            values.append(value)
            del values[:-self.WINDOW_SIZE]

    def status(self):
        psi_per_feature = {name: self._psi_for(name) for name in FEATURE_NAMES}
        max_psi = max(psi_per_feature.values(), default=0.0)
        return {
            "max_psi": max_psi,
            "level": self._level_for(max_psi),
            "features": psi_per_feature,
        }

    def _level_for(self, psi):
        if psi < PSI_OK_MAX:
            return "OK"
        if psi < PSI_WARN_MAX:
            return "WARN"
        return "ALERT"

    def _psi_for(self, feature_name):
        values = self.observed_values[feature_name]
        if len(values) < 10:
            return 0.0  # too little live data to judge

        edges = self.training_distribution[feature_name]
        bucket_edges = [edges["p10"], edges["p50"], edges["p90"]]
        actual_share = self._bucket_shares(values, bucket_edges)

        psi = 0.0
        for actual, expected in zip(actual_share, self.EXPECTED_BUCKET_SHARE):
            if actual > 0:  # an empty bucket adds nothing (and log(0) would crash)
                psi += (actual - expected) * math.log(actual / expected)
        return psi

    def _bucket_shares(self, values, edges):
        counts = [0, 0, 0, 0]
        for value in values:
            if value <= edges[0]:
                counts[0] += 1
            elif value <= edges[1]:
                counts[1] += 1
            elif value <= edges[2]:
                counts[2] += 1
            else:
                counts[3] += 1
        total = len(values)
        return [count / total for count in counts]


class FailureDetector:
    """Wraps the trained model. Give it an event's 4 numbers and it returns a
    risk score between 0 and 1. Shadow mode: the score is recorded, never
    acted on."""

    def __init__(self, model, training_distribution):
        self.model = model
        self.drift_monitor = DriftMonitor(training_distribution)

    @classmethod
    def load(cls, model_path, distribution_path):
        # Returns None if train.py hasn't been run yet, so the API still works without a model.
        if not model_path.exists() or not distribution_path.exists():
            return None
        model = joblib.load(model_path)
        training_distribution = json.loads(distribution_path.read_text())
        return cls(model, training_distribution)

    def predict_risk(self, features):
        # Also feeds the drift monitor, so every score doubles as a drift sample.
        self.drift_monitor.observe(features)
        return self.model.predict_proba([features])[0][1]
