"""One-click retrain pipeline: fresh dataset -> train challengers -> gate.

Run as a subprocess from POST /ml/retrain so sklearn training never blocks
the FastAPI event loop. Writes its verdict to artifacts/last_retrain_result.json.

Honesty note: each retrain regenerates the dataset with fresh random seeds
(same simulator, same fault definitions, new noise draws), so the
champion's original metrics and the challenger's metrics come from
different i.i.d. samples of the same underlying distribution rather than
a single frozen validation set. That's an appropriate simplification for
a demo-grade lifecycle, not full rigorous cross-validation.
"""
import json
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ml import generate_dataset, train_failure_model, train_rca_ranker

ML_DIR = os.path.dirname(os.path.abspath(__file__))
ARTIFACTS_ROOT = os.path.join(ML_DIR, "artifacts")
REGISTRY_PATH = os.path.join(ARTIFACTS_ROOT, "registry.json")
RESULT_PATH = os.path.join(ARTIFACTS_ROOT, "last_retrain_result.json")

MAX_LEAD_REGRESSION = 0.10  # challenger may not give >10% less warning than champion


def _read_registry():
    if not os.path.exists(REGISTRY_PATH):
        return {}
    with open(REGISTRY_PATH) as f:
        return json.load(f)


def _version_metrics(model_key, version):
    path = os.path.join(ARTIFACTS_ROOT, model_key, f"v{version}", "metrics.json")
    with open(path) as f:
        return json.load(f)


def _gate_failure_model(champion_version, challenger_version):
    champ = _version_metrics("failure_model", champion_version)["gbm"]
    chall = _version_metrics("failure_model", challenger_version)["gbm"]
    reasons = []
    if chall["pr_auc"] < champ["pr_auc"]:
        reasons.append(f"PR-AUC regressed ({chall['pr_auc']:.4f} < {champ['pr_auc']:.4f})")
    champ_lead = champ.get("median_lead_seconds") or 0
    chall_lead = chall.get("median_lead_seconds") or 0
    if champ_lead > 0 and chall_lead < champ_lead * (1 - MAX_LEAD_REGRESSION):
        reasons.append(f"median lead time regressed ({chall_lead:.0f}s < {champ_lead:.0f}s)")
    return (len(reasons) == 0), reasons


def _gate_rca_ranker(champion_version, challenger_version):
    champ = _version_metrics("rca_ranker", champion_version)
    chall = _version_metrics("rca_ranker", challenger_version)
    reasons = []
    if chall["ml_top1_accuracy"] < champ["ml_top1_accuracy"]:
        reasons.append(
            f"top-1 accuracy regressed ({chall['ml_top1_accuracy']:.2f} < {champ['ml_top1_accuracy']:.2f})")
    return (len(reasons) == 0), reasons


def main():
    seed_base = random.randint(1000, 999999)
    print(f"Generating fresh dataset (seed_base={seed_base})...", flush=True)
    generate_dataset.main(seed_base=seed_base)

    print("Training failure model challenger...", flush=True)
    train_failure_model.main()

    print("Training RCA ranker challenger...", flush=True)
    train_rca_ranker.main()

    registry = _read_registry()
    result = {"seed_base": seed_base, "models": {}}

    for model_key, gate_fn in (("failure_model", _gate_failure_model),
                               ("rca_ranker", _gate_rca_ranker)):
        entry = registry.get(model_key, {})
        versions = entry.get("versions", [])
        current_champion = entry.get("champion")
        if len(versions) < 2 or current_champion is None:
            continue
        challenger = versions[-1]
        if current_champion == challenger:
            continue
        passed, reasons = gate_fn(current_champion, challenger)
        if passed:
            entry["champion"] = challenger
        result["models"][model_key] = {
            "challenger_version": challenger,
            "previous_champion": current_champion,
            "promoted": passed,
            "reasons": reasons,
        }

    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2)
    with open(RESULT_PATH, "w") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
