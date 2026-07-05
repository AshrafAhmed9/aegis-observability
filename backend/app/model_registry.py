"""Read-only helpers for the versioned model registry
(ml/artifacts/registry.json). Training scripts and retrain_pipeline.py
write to it directly; this module only reads it for the API/frontend,
plus a small rollback helper.
"""
import json
import os
from typing import Optional

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTIFACTS_ROOT = os.path.join(BACKEND_DIR, "ml", "artifacts")
REGISTRY_PATH = os.path.join(ARTIFACTS_ROOT, "registry.json")


def _load_registry() -> dict:
    if not os.path.exists(REGISTRY_PATH):
        return {}
    with open(REGISTRY_PATH) as f:
        return json.load(f)


def _read_json(path) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def model_info(model_key: str) -> dict:
    registry = _load_registry()
    entry = registry.get(model_key, {"champion": None, "versions": []})
    versions_info = []
    for v in entry["versions"]:
        version_dir = os.path.join(ARTIFACTS_ROOT, model_key, f"v{v}")
        versions_info.append({
            "version": v,
            "is_champion": v == entry["champion"],
            "metrics": _read_json(os.path.join(version_dir, "metrics.json")),
            "train_meta": _read_json(os.path.join(version_dir, "train_meta.json")),
        })
    return {"champion": entry["champion"], "versions": versions_info}


def rollback(model_key: str, version: int) -> bool:
    registry = _load_registry()
    entry = registry.get(model_key)
    if not entry or version not in entry.get("versions", []):
        return False
    entry["champion"] = version
    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2)
    return True


def last_retrain_result() -> Optional[dict]:
    return _read_json(os.path.join(ARTIFACTS_ROOT, "last_retrain_result.json"))
