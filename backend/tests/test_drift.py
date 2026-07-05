import json
import random
import pytest
import app.drift as drift_mod
from app.drift import DriftMonitor


@pytest.fixture
def tmp_drift(tmp_path, monkeypatch):
    registry_path = tmp_path / "registry.json"
    dist_dir = tmp_path / "failure_model" / "v1"
    dist_dir.mkdir(parents=True)
    dist = {"feat_a": {"mean": 0.5, "std": 0.1, "p10": 0.3, "p50": 0.5, "p90": 0.7}}
    (dist_dir / "feature_distribution.json").write_text(json.dumps(dist))
    registry_path.write_text(json.dumps({"failure_model": {"champion": 1, "versions": [1]}}))
    monkeypatch.setattr(drift_mod, "ARTIFACTS_ROOT", str(tmp_path))
    monkeypatch.setattr(drift_mod, "REGISTRY_PATH", str(registry_path))


def test_drift_unavailable_without_registry(tmp_path, monkeypatch):
    monkeypatch.setattr(drift_mod, "REGISTRY_PATH", str(tmp_path / "missing.json"))
    monitor = DriftMonitor()
    assert monitor.available is False
    assert monitor.status()["available"] is False


def test_drift_stable_when_matching_distribution(tmp_drift):
    monitor = DriftMonitor()
    assert monitor.available is True
    rng = random.Random(1)
    # Reproduce the training distribution's actual bucket shape (10/40/40/10
    # around p10=0.3, p50=0.5, p90=0.7) rather than a uniform range —
    # otherwise the "expected" bucket proportions don't match at all and
    # PSI correctly reports drift.
    for _ in range(200):
        r = rng.random()
        if r < 0.10:
            value = rng.uniform(0.1, 0.3)
        elif r < 0.50:
            value = rng.uniform(0.3, 0.5)
        elif r < 0.90:
            value = rng.uniform(0.5, 0.7)
        else:
            value = rng.uniform(0.7, 0.9)
        monitor.observe({"feat_a": value})
    status = monitor.status()
    assert status["level"] in ("OK", "WARN")


def test_drift_alert_on_shifted_distribution(tmp_drift):
    monitor = DriftMonitor()
    for _ in range(100):
        monitor.observe({"feat_a": 5.0})
    status = monitor.status()
    assert status["level"] == "ALERT"
    assert status["max_psi"] > 0.25
