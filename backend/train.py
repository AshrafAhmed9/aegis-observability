"""
Trains the gradient boosting failure detector on simulator-generated
episodes -- this is the "self-training" step: Aegis generates its own
labeled data (see simulator.py) and learns from it, no manually collected
dataset required.

Run with: python train.py
"""

import json
from pathlib import Path

import joblib
from sklearn.ensemble import HistGradientBoostingClassifier

import ml
import simulator

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
MODEL_PATH = ARTIFACTS_DIR / "model.joblib"
DISTRIBUTION_PATH = ARTIFACTS_DIR / "feature_distribution.json"

TRAIN_EPISODES_PER_FAULT = 20   # of the 26 per fault, use 20 for training
VAL_EPISODES_PER_FAULT = 6      # and the remaining 6, unseen, for validation


def split_episodes():
    """Splits by whole episode, never by row -- so validation episodes are
    ones the model has never seen anything from, not just unseen rows
    within an episode it already trained on."""
    all_episodes = simulator.generate_episodes(episodes_per_fault=26)
    train_episodes, val_episodes = [], []
    counts_seen = {}
    for episode in all_episodes:
        fault_name = episode["fault_name"]
        counts_seen[fault_name] = counts_seen.get(fault_name, 0) + 1
        if counts_seen[fault_name] <= TRAIN_EPISODES_PER_FAULT:
            train_episodes.append(episode)
        else:
            val_episodes.append(episode)
    return train_episodes, val_episodes


def rows_for_episodes(episodes):
    rows = []
    for episode in episodes:
        rows.extend(ml.build_training_rows(episode))
    return rows


def train_model(train_rows):
    X = [features for features, _ in train_rows]
    y = [label for _, label in train_rows]
    model = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.1, max_iter=100, random_state=42)
    model.fit(X, y)
    return model


def measure_lead_times(model, val_episodes):
    """For each held-out episode, replays events in time order and records
    the first moment the model predicts high risk for the target service,
    then compares that to when the target actually failed."""
    leads = []
    for episode in val_episodes:
        events = sorted(episode["events"], key=lambda e: e["timestamp"])
        target_service = episode["root_cause_service"]
        seen = []
        prediction_time = None

        for event in events:
            if event["service"] == target_service and event["severity"] == "WARNING":
                features = ml.extract_features(seen, event, seen)
                probability = model.predict_proba([features])[0][1]
                if probability >= ml.RISK_THRESHOLD:
                    prediction_time = event["timestamp"]
            seen.append(event)

        if prediction_time is not None:
            leads.append(episode["failure_time"] - prediction_time)

    return leads


def median(values):
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def main():
    train_episodes, val_episodes = split_episodes()
    train_rows = rows_for_episodes(train_episodes)

    model = train_model(train_rows)
    leads = measure_lead_times(model, val_episodes)
    median_lead = median(leads)

    print(f"Trained on {len(train_rows)} rows from {len(train_episodes)} episodes")
    print(f"Validated on {len(val_episodes)} unseen episodes")
    if median_lead is None:
        print(f"Median early-warning lead time: n/a (0/{len(val_episodes)} caught)")
    else:
        print(f"Median early-warning lead time: {median_lead:.1f}s ({len(leads)}/{len(val_episodes)} caught)")

    ARTIFACTS_DIR.mkdir(exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    distribution = ml.compute_feature_distribution([features for features, _ in train_rows])
    DISTRIBUTION_PATH.write_text(json.dumps(distribution, indent=2))
    print(f"Saved model to {MODEL_PATH}")
    print(f"Saved feature distribution to {DISTRIBUTION_PATH}")


if __name__ == "__main__":
    main()
