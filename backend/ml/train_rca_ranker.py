"""Train the RCA ranker: a pointwise classifier that scores each degraded
candidate service's likelihood of being the true root cause, using only
identity-free graph/telemetry features (no service names) — so it can't
memorize "postgres-db = deadlock" and must learn structural patterns.

Splits by EPISODE (incident) so both candidates from the same incident
always land on the same side of the train/val split.

Honest framing: on this synthetic dataset the deterministic Kahn baseline
(app/correlation.py) is already 100% correct (see kahn_top1_correct), a
direct result of this session's cascade-timing fix. The ranker's value
here is demonstrated by AGREEMENT with a correct baseline using only
learned graph features — not by beating it, since there's no headroom to
beat on data this clean.
"""
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn import __version__ as sklearn_version

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ml.features import RCA_FEATURES

ML_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(ML_DIR, "data", "rca_candidates.csv")
ARTIFACTS_ROOT = os.path.join(ML_DIR, "artifacts")
REGISTRY_PATH = os.path.join(ARTIFACTS_ROOT, "registry.json")


def split_by_episode(df, val_frac=0.2, seed=42):
    rng = np.random.RandomState(seed)
    episodes = list(df["episode_id"].unique())
    rng.shuffle(episodes)
    n_val = max(1, int(len(episodes) * val_frac))
    val_episodes = set(episodes[:n_val])
    is_val = df["episode_id"].isin(val_episodes)
    return df[~is_val].copy(), df[is_val].copy()


def top1_accuracy(df, proba_col="proba"):
    """Fraction of incidents where the highest-scored candidate is the
    true root cause."""
    correct = 0
    total = 0
    for episode_id, group in df.groupby("episode_id"):
        total += 1
        top_idx = group[proba_col].idxmax()
        if group.loc[top_idx, "label"] == 1:
            correct += 1
    return correct / total if total else None


def next_version(registry, model_key):
    versions = registry.get(model_key, {}).get("versions", [])
    return (max(versions) + 1) if versions else 1


def main():
    df = pd.read_csv(DATA_PATH)
    train_df, val_df = split_by_episode(df)

    X_train, y_train = train_df[RCA_FEATURES].values, train_df["label"].values
    X_val = val_df[RCA_FEATURES].values

    model = HistGradientBoostingClassifier(
        max_depth=3, learning_rate=0.1, max_iter=150,
        class_weight="balanced", random_state=42,
    )
    model.fit(X_train, y_train)

    val_df = val_df.copy()
    val_df["proba"] = model.predict_proba(X_val)[:, 1]

    ml_top1 = top1_accuracy(val_df)
    kahn_top1 = val_df.groupby("episode_id")["kahn_top1_correct"].first().mean()

    # Agreement: does ML's top pick match the same service Kahn picked
    # (i.e. the label==1 row, when Kahn itself got that incident right)?
    agree_count, agree_total = 0, 0
    for episode_id, group in val_df.groupby("episode_id"):
        agree_total += 1
        ml_pick = group.loc[group["proba"].idxmax(), "service"]
        true_root = group.loc[group["label"] == 1, "service"]
        kahn_correct_here = group["kahn_top1_correct"].iloc[0] == 1
        kahn_pick = true_root.iloc[0] if (kahn_correct_here and len(true_root)) else None
        if kahn_pick is not None and ml_pick == kahn_pick:
            agree_count += 1
    agreement_rate = agree_count / agree_total if agree_total else None

    metrics = {
        "ml_top1_accuracy": ml_top1,
        "kahn_top1_accuracy": float(kahn_top1),
        "ml_kahn_agreement_rate": agreement_rate,
        "n_val_incidents": int(val_df["episode_id"].nunique()),
    }
    print(json.dumps(metrics, indent=2))

    os.makedirs(ARTIFACTS_ROOT, exist_ok=True)
    registry = {}
    if os.path.exists(REGISTRY_PATH):
        with open(REGISTRY_PATH) as f:
            registry = json.load(f)

    version = next_version(registry, "rca_ranker")
    version_dir = os.path.join(ARTIFACTS_ROOT, "rca_ranker", f"v{version}")
    os.makedirs(version_dir, exist_ok=True)

    joblib.dump(model, os.path.join(version_dir, "model.joblib"))
    with open(os.path.join(version_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)
    with open(os.path.join(version_dir, "feature_schema.json"), "w") as f:
        json.dump({"features": RCA_FEATURES}, f, indent=2)
    with open(os.path.join(version_dir, "train_meta.json"), "w") as f:
        json.dump({
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "sklearn_version": sklearn_version,
            "n_train_rows": len(train_df), "n_val_rows": len(val_df),
            "n_train_episodes": int(train_df["episode_id"].nunique()),
            "n_val_episodes": int(val_df["episode_id"].nunique()),
        }, f, indent=2)

    registry.setdefault("rca_ranker", {"champion": None, "versions": []})
    registry["rca_ranker"]["versions"].append(version)
    if registry["rca_ranker"]["champion"] is None:
        registry["rca_ranker"]["champion"] = version
        print(f"\nv{version} is the first version — auto-promoted as champion.")

    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2)
    print(f"Saved to {version_dir}")


if __name__ == "__main__":
    main()
