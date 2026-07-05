"""Confirms the ML layer degrades silently — no ML deps, no artifacts, or
no champion set should never break the base system, only disable the
ML-specific feature."""
import json
import app.ml_predictor as mlp
import app.rca_ranker as ranker


def test_ml_predictor_none_without_registry(tmp_path, monkeypatch):
    monkeypatch.setattr(mlp, "REGISTRY_PATH", str(tmp_path / "missing.json"))
    assert mlp.MLFailureDetector.load() is None


def test_ml_predictor_none_when_champion_unset(tmp_path, monkeypatch):
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(json.dumps({"failure_model": {"champion": None, "versions": []}}))
    monkeypatch.setattr(mlp, "REGISTRY_PATH", str(registry_path))
    monkeypatch.setattr(mlp, "ARTIFACTS_ROOT", str(tmp_path))
    assert mlp.MLFailureDetector.load() is None


def test_ml_predictor_none_when_deps_missing(monkeypatch):
    monkeypatch.setattr(mlp, "HAS_ML_DEPS", False)
    assert mlp.MLFailureDetector.load() is None


def test_rca_ranker_none_without_registry(tmp_path, monkeypatch):
    monkeypatch.setattr(ranker, "REGISTRY_PATH", str(tmp_path / "missing.json"))
    monkeypatch.setattr(ranker, "_loaded", False)
    monkeypatch.setattr(ranker, "_model", None)
    assert ranker.score_candidates([], [], []) is None


def test_rca_ranker_none_when_deps_missing(monkeypatch):
    monkeypatch.setattr(ranker, "HAS_ML_DEPS", False)
    monkeypatch.setattr(ranker, "_loaded", False)
    monkeypatch.setattr(ranker, "_model", None)
    assert ranker.score_candidates([], [], []) is None
