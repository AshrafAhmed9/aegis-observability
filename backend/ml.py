"""
A gradient-boosted classifier that watches for early warning signs of
failure -- it runs alongside the deterministic correlation engine ("shadow
mode"), never overriding it, just scored against what actually happens.

Also includes drift detection: if live traffic starts looking statistically
different from what the model was trained on, that's a sign the model may
no longer be reliable.
"""

import json
import math

import joblib

FEATURE_NAMES = ["warning_count", "error_count", "seconds_since_last_event", "degraded_services_count"]

# The model is only ever trained on these three services (see
# build_training_rows) -- "api" only appears as a downstream cascade
# symptom, never as its own labeled example, so a prediction on "api" would
# be an untrained extrapolation, not a meaningful score.
TRAINED_SERVICES = {"cache", "queue", "database"}

# A prediction only counts if we're at least this confident.
RISK_THRESHOLD = 0.5

# How PSI (Population Stability Index) buckets are labeled, using the
# thresholds analysts commonly use to flag distribution drift.
PSI_OK_MAX = 0.1
PSI_WARN_MAX = 0.25


def extract_features(prior_events, current_event):
    """Builds the feature vector for current_event, using only events that
    happened before it -- a live system can't see the future, so neither
    should training data."""
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
    """Turns one episode into labeled (features, label) rows.

    Positive rows: the target service's WARNING event -- this is the
    moment a real predictor would need to raise an alarm.

    Negative rows: baseline events from services that never fail in this
    episode -- a clean example of "nothing is wrong here".
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
    """Summarizes each feature's spread in the training data as
    (p10, p50, p90) -- the same three quantiles used later to check for
    drift in live traffic."""
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
    """Compares live feature values against the training distribution using
    PSI (Population Stability Index): split each feature's range into 4
    buckets using the training p10/p50/p90 as edges, then compare how often
    live values land in each bucket versus how often training values did.
    A big mismatch means the live data no longer looks like what the model
    was trained on."""

    EXPECTED_BUCKET_SHARE = [0.10, 0.40, 0.40, 0.10]
    WINDOW_SIZE = 200  # compare against recent traffic, not everything ever seen

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
            return 0.0  # not enough live data yet to say anything meaningful

        edges = self.training_distribution[feature_name]
        bucket_edges = [edges["p10"], edges["p50"], edges["p90"]]
        actual_share = self._bucket_shares(values, bucket_edges)

        psi = 0.0
        for actual, expected in zip(actual_share, self.EXPECTED_BUCKET_SHARE):
            if actual > 0:  # an empty bucket contributes nothing
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
    """Loads the trained gradient boosting model and scores live feature
    vectors. Runs in shadow mode: its predictions are recorded and compared
    against what actually happens, but the deterministic engine is always
    the one acting on the data."""

    def __init__(self, model, training_distribution):
        self.model = model
        self.drift_monitor = DriftMonitor(training_distribution)

    @classmethod
    def load(cls, model_path, distribution_path):
        if not model_path.exists() or not distribution_path.exists():
            return None
        model = joblib.load(model_path)
        training_distribution = json.loads(distribution_path.read_text())
        return cls(model, training_distribution)

    def predict_risk(self, features):
        self.drift_monitor.observe(features)
        probability = self.model.predict_proba([features])[0][1]
        return probability

    def is_high_risk(self, features):
        return self.predict_risk(features) >= RISK_THRESHOLD
