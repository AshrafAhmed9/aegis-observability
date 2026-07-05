import json
import pytest
import app.incident_memory as im_mod
from app.incident_memory import IncidentMemory


@pytest.fixture
def tmp_corpus(tmp_path, monkeypatch):
    corpus = {
        "incidents": [
            {"title": "Redis A", "fault_class": "resource_exhaustion",
             "signature": "root cause class: resource_exhaustion. degraded services: redis-cache. "
                          "error classes: redis.exceptions.ConnectionError"},
            {"title": "Redis B", "fault_class": "resource_exhaustion",
             "signature": "root cause class: resource_exhaustion. degraded services: redis-cache, api-gateway. "
                          "error classes: redis.exceptions.ConnectionError"},
            {"title": "Deadlock A", "fault_class": "deadlock",
             "signature": "root cause class: deadlock. degraded services: postgres-db. "
                          "error classes: postgres.deadlock"},
        ]
    }
    path = tmp_path / "incident_corpus.json"
    path.write_text(json.dumps(corpus))
    monkeypatch.setattr(im_mod, "CORPUS_PATH", str(path))


def test_load_returns_none_without_corpus(tmp_path, monkeypatch):
    monkeypatch.setattr(im_mod, "CORPUS_PATH", str(tmp_path / "missing.json"))
    assert IncidentMemory.load() is None


def test_load_returns_none_when_numpy_missing(monkeypatch):
    monkeypatch.setattr(im_mod, "HAS_NUMPY", False)
    assert IncidentMemory.load() is None


def test_similarity_clusters_same_fault_class(tmp_corpus):
    memory = IncidentMemory.load()
    assert memory is not None
    query = ("root cause class: resource_exhaustion. degraded services: redis-cache. "
             "error classes: redis.exceptions.ConnectionError")
    results = memory.similar(query, top_k=3)
    assert results[0]["fault_class"] == "resource_exhaustion"
    fault_classes = [r["fault_class"] for r in results[:2]]
    assert "deadlock" not in fault_classes


def test_add_appends_and_is_retrievable(tmp_corpus):
    memory = IncidentMemory.load()
    memory.add({"title": "New Queue Incident", "fault_class": "resource_exhaustion",
                "signature": "root cause class: resource_exhaustion. degraded services: payment-worker. "
                             "error classes: celery.exceptions.WorkerLostError"})
    results = memory.similar(
        "root cause class: resource_exhaustion. degraded services: payment-worker. "
        "error classes: celery.exceptions.WorkerLostError", top_k=1)
    assert results[0]["title"] == "New Queue Incident"
