"""
Grades the correlation engine: generates 78 labeled fault episodes, runs each
one through the real Correlator + rca pipeline, and checks whether the
predicted root cause matches the service we actually broke.

Run with: python run_eval.py
"""

from pathlib import Path

import ml
import rca
import simulator
from correlator import Correlator

MODEL_PATH = Path(__file__).resolve().parent / "artifacts" / "model.joblib"
DISTRIBUTION_PATH = Path(__file__).resolve().parent / "artifacts" / "feature_distribution.json"


def run_episode_through_pipeline(events):
    """Feeds one episode's events through the real correlator and RCA
    engine, exactly as production would. Returns the RCA result."""
    correlator = Correlator()
    for event in events:
        correlator.add_event(event)
    sealed_traces = correlator.flush_all()
    trace_events = sealed_traces[0]  # one trace_id per episode, so exactly one trace
    return rca.analyze(trace_events)


def evaluate_correctness():
    episodes = simulator.generate_episodes()
    results = []
    for episode in episodes:
        result = run_episode_through_pipeline(episode["events"])
        ranked = result["ranked_root_causes"]
        predicted = ranked[0] if ranked else None
        results.append({
            "fault_name": episode["fault_name"],
            "seed": episode["seed"],
            "expected": episode["root_cause_service"],
            "predicted": predicted,
            "correct": predicted == episode["root_cause_service"],
        })
    return results


def write_scorecard(results, path):
    correct_count = sum(1 for r in results if r["correct"])
    total = len(results)
    lines = [
        "# Aegis Evaluation Scorecard",
        "",
        "## RCA Correctness",
        f"{correct_count}/{total} correct",
        "",
    ]
    failures = [r for r in results if not r["correct"]]
    if failures:
        lines.append("### Misses")
        for r in failures:
            lines.append(f"- {r['fault_name']} (seed {r['seed']}): expected {r['expected']}, got {r['predicted']}")
    path.write_text("\n".join(lines) + "\n")


def evaluate_lead_time():
    """Loads the trained model and measures its early-warning lead time on
    the same held-out episodes train.py validated against. Returns None
    if no model has been trained yet."""
    from train import measure_lead_times, median, split_episodes

    detector = ml.FailureDetector.load(MODEL_PATH, DISTRIBUTION_PATH)
    if detector is None:
        return None

    _, val_episodes = split_episodes()
    leads = measure_lead_times(detector.model, val_episodes)
    return median(leads), len(leads), len(val_episodes)


def main():
    results = evaluate_correctness()
    correct_count = sum(1 for r in results if r["correct"])
    total = len(results)
    print(f"RCA correctness: {correct_count}/{total}")

    scorecard_path = Path(__file__).resolve().parent / "scorecard.md"
    write_scorecard(results, scorecard_path)
    print(f"Wrote {scorecard_path}")

    lead_result = evaluate_lead_time()
    if lead_result is None:
        print("No trained model found -- run `python -m ml.train` first.")
    else:
        median_lead, caught, total_val = lead_result
        print(f"Median early-warning lead time: {median_lead:.1f}s ({caught}/{total_val} caught)")


if __name__ == "__main__":
    main()
