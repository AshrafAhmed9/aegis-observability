"""Train the failure-prediction model (challenger to the stats baseline).

Splits by EPISODE (never by row) so no window from the same simulated
incident appears in both train and validation.
"""
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import HistGradientBoostingClassifier, IsolationForest
from sklearn.metrics import average_precision_score, precision_recall_curve
from sklearn import __version__ as sklearn_version

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ml.features import FAILURE_FEATURES

ML_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(ML_DIR, "data", "failure_windows.csv")
ARTIFACTS_ROOT = os.path.join(ML_DIR, "artifacts")
REGISTRY_PATH = os.path.join(ARTIFACTS_ROOT, "registry.json")

MIN_PRECISION = 0.9


def split_by_episode(df, val_frac=0.2, seed=42):
    rng = np.random.RandomState(seed)
    val_episodes = set()
    for fault, group in df.groupby("fault"):
        episodes = list(group["episode_id"].unique())
        rng.shuffle(episodes)
        n_val = max(1, int(len(episodes) * val_frac))
        val_episodes.update(episodes[:n_val])
    is_val = df["episode_id"].isin(val_episodes)
    return df[~is_val].copy(), df[is_val].copy()


def pick_threshold(y_true, y_proba, min_precision=MIN_PRECISION):
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    precision, recall = precision[:-1], recall[:-1]
    candidates = [(t, p, r) for t, p, r in zip(thresholds, precision, recall) if p >= min_precision]
    if not candidates:
        best_idx = int(np.argmax(precision))
        return float(thresholds[best_idx]), float(precision[best_idx]), float(recall[best_idx])
    t, p, r = max(candidates, key=lambda c: c[2])
    return float(t), float(p), float(r)


def calibration_curve(y_true, y_proba, n_bins=10):
    """Reliability diagram data: for each probability bin, the model's mean
    predicted probability vs. the actual observed positive rate. A
    well-calibrated model has mean_predicted ≈ actual_rate in every bin —
    i.e. "when the model says 80%, it's right about 80% of the time."
    """
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    bin_ids = np.clip(np.digitize(y_proba, bins) - 1, 0, n_bins - 1)
    curve = []
    for b in range(n_bins):
        mask = bin_ids == b
        count = int(mask.sum())
        if count == 0:
            continue
        curve.append({
            "bin_lo": round(float(bins[b]), 2),
            "bin_hi": round(float(bins[b + 1]), 2),
            "count": count,
            "mean_predicted": float(y_proba[mask].mean()),
            "actual_rate": float(y_true[mask].mean()),
        })
    return curve


def expected_calibration_error(curve, n_total):
    """Weighted mean absolute gap between predicted confidence and actual
    outcome rate across bins — the standard single-number calibration
    metric (lower is better; 0 is perfect)."""
    if n_total == 0:
        return None
    return sum(abs(c["mean_predicted"] - c["actual_rate"]) * c["count"] for c in curve) / n_total


def brier_score(y_true, y_proba):
    """Mean squared error between predicted probability and actual outcome
    (0/1) — rewards both calibration and sharpness. Lower is better."""
    return float(np.mean((y_proba - y_true) ** 2))


def evaluate(y_true, y_proba, seconds_to_onset, threshold):
    _, precision, recall = pick_threshold(y_true, y_proba)
    pr_auc = average_precision_score(y_true, y_proba)
    flagged_true_positive = (y_true == 1) & (y_proba >= threshold)
    leads = seconds_to_onset[flagged_true_positive]
    leads = leads[leads > 0]
    return {
        "pr_auc": float(pr_auc),
        "threshold": threshold,
        "precision_at_threshold": precision,
        "recall_at_threshold": recall,
        "median_lead_seconds": float(np.median(leads)) if len(leads) else None,
        "n_true_positives_caught": int(flagged_true_positive.sum()),
        "n_positives_total": int((y_true == 1).sum()),
    }


def next_version(registry, model_key):
    versions = registry.get(model_key, {}).get("versions", [])
    return (max(versions) + 1) if versions else 1


def main():
    df = pd.read_csv(DATA_PATH)
    train_df, val_df = split_by_episode(df)

    X_train, y_train = train_df[FAILURE_FEATURES].values, train_df["label"].values
    X_val, y_val = val_df[FAILURE_FEATURES].values, val_df["label"].values
    val_seconds_to_onset = val_df["seconds_to_onset"].values

    gbm = HistGradientBoostingClassifier(
        max_depth=4, learning_rate=0.08, max_iter=200,
        class_weight="balanced", random_state=42,
    )
    gbm.fit(X_train, y_train)
    gbm_proba = gbm.predict_proba(X_val)[:, 1]
    gbm_threshold, _, _ = pick_threshold(y_val, gbm_proba)
    gbm_metrics = evaluate(y_val, gbm_proba, val_seconds_to_onset, gbm_threshold)

    # Calibration: is the GBM's confidence trustworthy, not just its ranking?
    # PR-AUC/precision/recall only measure ranking quality — a model can
    # separate classes perfectly while still being wildly overconfident.
    calib_curve = calibration_curve(y_val, gbm_proba)
    gbm_metrics["calibration"] = {
        "curve": calib_curve,
        "ece": expected_calibration_error(calib_curve, len(y_val)),
        "brier_score": brier_score(y_val, gbm_proba),
    }

    # Unsupervised comparison row: trained ignoring labels, scored on the
    # same held-out set for an apples-to-apples PR-AUC/lead-time entry.
    iso = IsolationForest(n_estimators=200, contamination="auto", random_state=42)
    iso.fit(X_train)
    iso_risk = -iso.score_samples(X_val)  # higher = more anomalous
    iso_threshold, _, _ = pick_threshold(y_val, iso_risk)
    iso_metrics = evaluate(y_val, iso_risk, val_seconds_to_onset, iso_threshold)

    print("=== GBM (supervised challenger) ===")
    print(json.dumps(gbm_metrics, indent=2))
    print(f"    calibration: ECE={gbm_metrics['calibration']['ece']:.4f}  "
          f"Brier={gbm_metrics['calibration']['brier_score']:.4f} (lower is better, 0 = perfect)")
    print("=== IsolationForest (unsupervised comparison) ===")
    print(json.dumps(iso_metrics, indent=2))

    os.makedirs(ARTIFACTS_ROOT, exist_ok=True)
    registry = {}
    if os.path.exists(REGISTRY_PATH):
        with open(REGISTRY_PATH) as f:
            registry = json.load(f)

    version = next_version(registry, "failure_model")
    version_dir = os.path.join(ARTIFACTS_ROOT, "failure_model", f"v{version}")
    os.makedirs(version_dir, exist_ok=True)

    joblib.dump(gbm, os.path.join(version_dir, "model.joblib"))

    with open(os.path.join(version_dir, "metrics.json"), "w") as f:
        json.dump({"gbm": gbm_metrics, "isolation_forest": iso_metrics}, f, indent=2)

    with open(os.path.join(version_dir, "feature_schema.json"), "w") as f:
        json.dump({"features": FAILURE_FEATURES}, f, indent=2)

    # Feature distribution snapshot for drift monitoring (Phase E).
    dist = {}
    for col in FAILURE_FEATURES:
        vals = train_df[col].values
        dist[col] = {
            "mean": float(np.mean(vals)), "std": float(np.std(vals)) or 1e-6,
            "p10": float(np.percentile(vals, 10)), "p50": float(np.percentile(vals, 50)),
            "p90": float(np.percentile(vals, 90)),
        }
    with open(os.path.join(version_dir, "feature_distribution.json"), "w") as f:
        json.dump(dist, f, indent=2)

    with open(os.path.join(version_dir, "train_meta.json"), "w") as f:
        json.dump({
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "sklearn_version": sklearn_version,
            "n_train_rows": len(train_df), "n_val_rows": len(val_df),
            "n_train_episodes": int(train_df["episode_id"].nunique()),
            "n_val_episodes": int(val_df["episode_id"].nunique()),
        }, f, indent=2)

    registry.setdefault("failure_model", {"champion": None, "versions": []})
    registry["failure_model"]["versions"].append(version)
    if registry["failure_model"]["champion"] is None:
        registry["failure_model"]["champion"] = version
        print(f"\nv{version} is the first version — auto-promoted as champion.")
    else:
        print(f"\nv{version} trained (not auto-promoted; use the retrain gate to compare).")

    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2)

    print(f"Saved to {version_dir}")


if __name__ == "__main__":
    main()
