"""
Grades the project. Two checks:
1. Root cause: makes 78 outages where we know which service broke, runs each
   through the real Correlator and rca code, and counts how often the
   engine names the right service.
2. Early warning: how many seconds before failure the ML model raises its
   first alarm, on episodes it was not trained on.

Results are recomputed from scratch on every run; nothing is hardcoded.

Run with: python run_eval.py
"""

from pathlib import Path

import ml
import rca
import simulator
from correlator import Correlator
from train import DISTRIBUTION_PATH, MODEL_PATH, describe_lead_time, measure_lead_times, split_episodes


def run_episode_through_pipeline(events):
    """Runs one episode through the same Correlator and rca code the API uses.
    Returns the RCA result."""
    correlator = Correlator()
    for event in events:
        correlator.add_event(event)
    sealed_traces = correlator.flush_all()
    trace_events = sealed_traces[0]  # an episode is one trace, so there is exactly one
    return rca.analyze(trace_events)


def evaluate_correctness():
    """Runs all 78 episodes and records the expected vs predicted root cause for each."""
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
    """Writes scorecard.md: the score, plus any misses with their seed so they can be reproduced."""
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
    """Measures the model's early-warning lead times on the held-out
    episodes. Returns (lead_times, episode_count), or None if train.py
    hasn't been run yet."""
    detector = ml.FailureDetector.load(MODEL_PATH, DISTRIBUTION_PATH)
    if detector is None:
        return None

    _, val_episodes = split_episodes()
    leads = measure_lead_times(detector.model, val_episodes)
    return leads, len(val_episodes)


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
        print("No trained model found -- run `python train.py` first.")
    else:
        leads, total_val = lead_result
        print(describe_lead_time(leads, total_val))


if __name__ == "__main__":
    main()
