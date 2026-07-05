"""Runtime inference for the trained failure-prediction model.

Mirrors FailurePredictor's Prediction dataclass and active()/TTL/dedup
pattern so main.py and consumer.py can merge both detectors' outputs
without special-casing either. All ML imports are guarded — with no ML
deps or artifacts installed, MLFailureDetector.load() returns None and
the rest of the system runs exactly as it did before this module existed.
"""
import json
import os
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from .predictor import Prediction

try:
    import joblib
    HAS_ML_DEPS = True
except ImportError:
    HAS_ML_DEPS = False

ML_RISK = "ML_RISK"

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTIFACTS_ROOT = os.path.join(BACKEND_DIR, "ml", "artifacts")
REGISTRY_PATH = os.path.join(ARTIFACTS_ROOT, "registry.json")

WINDOW_SECONDS = 60.0
PREDICTION_TTL_SECONDS = 120.0
RISK_THRESHOLD = 0.5
RE_ARM_BELOW = 0.3
HORIZON_SECONDS = 120.0


def _champion_version_dir() -> Optional[str]:
    if not os.path.exists(REGISTRY_PATH):
        return None
    with open(REGISTRY_PATH) as f:
        registry = json.load(f)
    entry = registry.get("failure_model")
    if not entry or entry.get("champion") is None:
        return None
    version = entry["champion"]
    version_dir = os.path.join(ARTIFACTS_ROOT, "failure_model", f"v{version}")
    return version_dir if os.path.isdir(version_dir) else None


class MLFailureDetector:
    def __init__(self, model, feature_names: List[str], version: int, drift_monitor=None):
        self.model = model
        self.feature_names = feature_names
        self.version = version
        self.drift_monitor = drift_monitor
        self._buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        self._active: Dict[tuple, Prediction] = {}
        self._watermark = float("-inf")

    @classmethod
    def load(cls, drift_monitor=None) -> Optional["MLFailureDetector"]:
        if not HAS_ML_DEPS:
            return None
        version_dir = _champion_version_dir()
        if version_dir is None:
            return None
        try:
            model = joblib.load(os.path.join(version_dir, "model.joblib"))
            with open(os.path.join(version_dir, "feature_schema.json")) as f:
                schema = json.load(f)
            version = int(os.path.basename(version_dir).lstrip("v"))
            return cls(model, schema["features"], version, drift_monitor=drift_monitor)
        except Exception:
            return None

    def observe(self, event: dict) -> List[Prediction]:
        from ml.features import window_features  # local import: only needed if loaded

        service = event.get("service")
        ts = event.get("_event_ts")
        if not service or ts is None:
            return []
        self._watermark = max(self._watermark, ts)

        buf = self._buffers[service]
        buf.append(event)
        while buf and buf[0]["_event_ts"] < ts - WINDOW_SECONDS - 5.0:
            buf.popleft()

        feats = window_features(list(buf), ts, WINDOW_SECONDS)
        if feats is None:
            return []

        if self.drift_monitor is not None:
            self.drift_monitor.observe(feats)

        vector = [[feats.get(name, 0.0) for name in self.feature_names]]
        proba = float(self.model.predict_proba(vector)[0, 1])

        key = (service, ML_RISK)
        emitted = []
        if proba >= RISK_THRESHOLD:
            pred = Prediction(
                service=service, metric="ml_risk_score", kind=ML_RISK,
                current_value=proba, threshold=RISK_THRESHOLD,
                eta_seconds=HORIZON_SECONDS, predicted_at=ts, confidence=proba,
                severity="CRITICAL" if proba >= 0.85 else "WARNING",
                summary=f"ML: {int(proba * 100)}% failure risk for {service} within {int(HORIZON_SECONDS)}s (v{self.version})",
            )
            self._active[key] = pred
            emitted.append(pred)
        elif proba <= RE_ARM_BELOW:
            self._active.pop(key, None)

        return emitted

    def active(self) -> List[Prediction]:
        now = self._watermark
        live = []
        for key, pred in list(self._active.items()):
            if now - pred.predicted_at > PREDICTION_TTL_SECONDS:
                del self._active[key]
                continue
            live.append(pred)
        return live
