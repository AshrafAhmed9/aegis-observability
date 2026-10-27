"""
Trains the failure-prediction model.

There is no dataset to download. simulator.py makes outages where the answer
is known, and the model learns from those.

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

EPISODES_PER_FAULT = 26
TRAIN_EPISODES_PER_FAULT = 20   # the other 6 per fault are held back to test on


def split_episodes():
    """Splits into training and held-out episodes. The split is by whole
    episode, so the held-out ones are completely new to the model."""
    all_episodes = simulator.generate_episodes(episodes_per_fault=EPISODES_PER_FAULT)
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
    """All the training examples from a list of episodes."""
    rows = []
    for episode in episodes:
        rows.extend(ml.build_training_rows(episode))
    return rows


def train_model(train_rows):
    """Fits the gradient-boosting model on (numbers, label) examples."""
    X = [features for features, _ in train_rows]
    y = [label for _, label in train_rows]
    model = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.1, max_iter=100, random_state=42)
    model.fit(X, y)
    return model


def measure_lead_times(model, val_episodes):
    """How early does the model warn? For each held-out episode, replay the
    events in order and note when the model flags the failing service as high
    risk. Lead time = actual failure time minus that moment. Episodes the
    model never flags are left out."""
    leads = []
    for episode in val_episodes:
        events = sorted(episode["events"], key=lambda e: e["timestamp"])
        target_service = episode["root_cause_service"]
        seen = []
        prediction_time = None

        for event in events:
            if event["service"] == target_service and event["severity"] == "WARNING":
                features = ml.extract_features(seen, event)
                probability = model.predict_proba([features])[0][1]
                if probability >= ml.RISK_THRESHOLD:
                    prediction_time = event["timestamp"]
            seen.append(event)

        if prediction_time is not None:
            leads.append(episode["failure_time"] - prediction_time)

    return leads


def median(values):
    """Middle value of a list, or None if it is empty."""
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def describe_lead_time(leads, total_episodes):
    """One line summarizing the early-warning result (also used by run_eval.py)."""
    if not leads:
        return f"Median early-warning lead time: n/a (0/{total_episodes} caught)"
    return f"Median early-warning lead time: {median(leads):.1f}s ({len(leads)}/{total_episodes} caught)"


def main():
    train_episodes, val_episodes = split_episodes()
    train_rows = rows_for_episodes(train_episodes)

    model = train_model(train_rows)
    leads = measure_lead_times(model, val_episodes)

    print(f"Trained on {len(train_rows)} rows from {len(train_episodes)} episodes")
    print(f"Validated on {len(val_episodes)} unseen episodes")
    print(describe_lead_time(leads, len(val_episodes)))

    ARTIFACTS_DIR.mkdir(exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    distribution = ml.compute_feature_distribution([features for features, _ in train_rows])
    DISTRIBUTION_PATH.write_text(json.dumps(distribution, indent=2))
    print(f"Saved model to {MODEL_PATH}")
    print(f"Saved feature distribution to {DISTRIBUTION_PATH}")


if __name__ == "__main__":
    main()
