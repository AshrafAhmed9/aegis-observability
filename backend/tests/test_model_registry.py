import json
import pytest
from app import model_registry


@pytest.fixture
def tmp_registry(tmp_path, monkeypatch):
    registry_path = tmp_path / "registry.json"
    monkeypatch.setattr(model_registry, "ARTIFACTS_ROOT", str(tmp_path))
    monkeypatch.setattr(model_registry, "REGISTRY_PATH", str(registry_path))
    return registry_path


def test_model_info_empty_registry(tmp_registry):
    info = model_registry.model_info("failure_model")
    assert info == {"champion": None, "versions": []}


def test_model_info_reads_versions_and_metrics(tmp_registry, tmp_path):
    registry = {"failure_model": {"champion": 1, "versions": [1, 2]}}
    tmp_registry.write_text(json.dumps(registry))
    for v in (1, 2):
        vdir = tmp_path / "failure_model" / f"v{v}"
        vdir.mkdir(parents=True)
        (vdir / "metrics.json").write_text(json.dumps({"gbm": {"pr_auc": 0.9}}))
    info = model_registry.model_info("failure_model")
    assert info["champion"] == 1
    assert len(info["versions"]) == 2
    assert info["versions"][0]["is_champion"] is True
    assert info["versions"][1]["is_champion"] is False
    assert info["versions"][0]["metrics"]["gbm"]["pr_auc"] == 0.9


def test_rollback_success_and_failure(tmp_registry):
    registry = {"failure_model": {"champion": 2, "versions": [1, 2]}}
    tmp_registry.write_text(json.dumps(registry))
    assert model_registry.rollback("failure_model", 1) is True
    assert model_registry.model_info("failure_model")["champion"] == 1
    assert model_registry.rollback("failure_model", 99) is False
    assert model_registry.rollback("unknown_model", 1) is False
