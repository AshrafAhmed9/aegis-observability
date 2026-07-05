"""Runtime inference for the trained RCA ranker.

Augments (never replaces) the deterministic Kahn root-cause result in
correlation.py — that result remains the system's answer; this ranking is
displayed alongside it as an independent, learned confirmation signal.

Guarded imports: with no ML deps/artifacts, score_candidates() returns
None and callers skip attaching ml_ranking — the rest of the pipeline is
unaffected.
"""
import json
import os
from typing import List, Optional

try:
    import joblib
    HAS_ML_DEPS = True
except ImportError:
    HAS_ML_DEPS = False

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTIFACTS_ROOT = os.path.join(BACKEND_DIR, "ml", "artifacts")
REGISTRY_PATH = os.path.join(ARTIFACTS_ROOT, "registry.json")

_model = None
_feature_names = None
_version = None
_loaded = False


def _champion_version_dir() -> Optional[str]:
    if not os.path.exists(REGISTRY_PATH):
        return None
    with open(REGISTRY_PATH) as f:
        registry = json.load(f)
    entry = registry.get("rca_ranker")
    if not entry or entry.get("champion") is None:
        return None
    version = entry["champion"]
    version_dir = os.path.join(ARTIFACTS_ROOT, "rca_ranker", f"v{version}")
    return version_dir if os.path.isdir(version_dir) else None


def _ensure_loaded():
    global _model, _feature_names, _version, _loaded
    if _loaded:
        return
    _loaded = True
    if not HAS_ML_DEPS:
        return
    version_dir = _champion_version_dir()
    if version_dir is None:
        return
    try:
        _model = joblib.load(os.path.join(version_dir, "model.joblib"))
        with open(os.path.join(version_dir, "feature_schema.json")) as f:
            _feature_names = json.load(f)["features"]
        _version = int(os.path.basename(version_dir).lstrip("v"))
    except Exception:
        _model = None
def reload() -> None:
    """Force re-reading the registry and reloading the champion model —
    called after a retrain promotes a new version."""
    global _model, _feature_names, _version, _loaded
    _model, _feature_names, _version, _loaded = None, None, None, False
    _ensure_loaded()


def available() -> bool:
    _ensure_loaded()
    return _model is not None


def score_candidates(nodes: List[dict], edges: List[dict], topo_order: List[str]) -> Optional[dict]:
    """Returns {"version": N, "ranking": [{"service", "ml_proba"}, ...]}
    sorted by proba descending, or None if the model isn't available."""
    _ensure_loaded()
    if _model is None:
        return None
    from ml.features import candidate_features, SEV_LEVELS

    degraded = [n for n in nodes if SEV_LEVELS.get(n["max_severity"], 0) >= 2]
    if not degraded:
        return None

    vectors = [
        [candidate_features(node, nodes, edges, topo_order).get(name, 0.0)
         for name in _feature_names]
        for node in degraded
    ]

    probas = _model.predict_proba(vectors)[:, 1]
    ranking = sorted(
        [{"service": n["service_name"], "ml_proba": float(p)}
         for n, p in zip(degraded, probas)],
        key=lambda r: -r["ml_proba"],
    )
    return {"version": _version, "ranking": ranking}
