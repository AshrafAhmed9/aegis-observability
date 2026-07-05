"""Lightweight drift monitor: compares live inference feature
distributions against the champion model's training-time snapshot using
a coarse 4-bucket Population Stability Index (PSI) approximation (bucket
edges = the training p10/p50/p90 quantiles saved at train time).

PSI interpretation (standard industry thresholds):
  < 0.1    -> stable (OK)
  0.1-0.25 -> moderate shift (WARN)
  > 0.25   -> significant shift (ALERT)
"""
import json
import math
import os
from collections import deque
from typing import Dict, Optional

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTIFACTS_ROOT = os.path.join(BACKEND_DIR, "ml", "artifacts")
REGISTRY_PATH = os.path.join(ARTIFACTS_ROOT, "registry.json")

WINDOW_SIZE = 500
MIN_SAMPLES = 30
EPS = 1e-6


def _champion_distribution() -> Optional[dict]:
    if not os.path.exists(REGISTRY_PATH):
        return None
    with open(REGISTRY_PATH) as f:
        registry = json.load(f)
    entry = registry.get("failure_model")
    if not entry or entry.get("champion") is None:
        return None
    path = os.path.join(ARTIFACTS_ROOT, "failure_model", f"v{entry['champion']}", "feature_distribution.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


class DriftMonitor:
    def __init__(self):
        self._distribution = _champion_distribution()
        self._windows: Dict[str, deque] = {}

    @property
    def available(self) -> bool:
        return self._distribution is not None

    def observe(self, features: Dict[str, float]) -> None:
        if not self.available:
            return
        for name, value in features.items():
            if name not in self._distribution:
                continue
            self._windows.setdefault(name, deque(maxlen=WINDOW_SIZE)).append(value)

    def _psi_for(self, name: str) -> Optional[float]:
        window = self._windows.get(name)
        dist = self._distribution.get(name)
        if not window or not dist or len(window) < MIN_SAMPLES:
            return None
        p10, p50, p90 = dist["p10"], dist["p50"], dist["p90"]
        expected = [0.10, 0.40, 0.40, 0.10]
        buckets = [0, 0, 0, 0]
        for v in window:
            if v < p10:
                buckets[0] += 1
            elif v < p50:
                buckets[1] += 1
            elif v < p90:
                buckets[2] += 1
            else:
                buckets[3] += 1
        total = len(window)
        actual = [b / total for b in buckets]
        return sum((a - e) * math.log((a + EPS) / (e + EPS)) for a, e in zip(actual, expected))

    def status(self) -> dict:
        if not self.available:
            return {"available": False, "level": None, "max_psi": None, "features": {}}
        features = {}
        max_psi = 0.0
        for name in self._distribution:
            psi = self._psi_for(name)
            if psi is None:
                continue
            features[name] = round(psi, 4)
            max_psi = max(max_psi, psi)
        level = "OK"
        if max_psi > 0.25:
            level = "ALERT"
        elif max_psi > 0.1:
            level = "WARN"
        return {"available": True, "level": level, "max_psi": round(max_psi, 4), "features": features}
