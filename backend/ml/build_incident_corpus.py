"""Build a corpus of past incidents for similarity search.

Each entry stores a SIGNATURE TEXT (root-cause class, degraded services,
error classes, representative messages — NOT raw timestamps, so
similarity reflects "what kind of incident" rather than "when did it
happen") plus metadata. Vectors are computed at LOAD time by
app/incident_memory.py, not stored here — this keeps the corpus
encoder-agnostic (works whether sentence-transformers is installed or not).
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.correlation import CorrelationEngine
from app.parser import parse_log_line
from app.streaming import parse_event_time
from app.simulator import FAULTS
from ml.generate_dataset import run_episode, failure_onset, HORIZON
from ml.features import incident_signature

ML_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DIR = os.path.join(os.path.dirname(ML_DIR), "sample_logs")
EVAL_DIR = os.path.join(os.path.dirname(ML_DIR), "eval")
ARTIFACTS_ROOT = os.path.join(ML_DIR, "artifacts")


def _entry_from_events(incident_events, title, fallback_fault_class=None):
    nodes, edges = CorrelationEngine.build_propagation_graph(incident_events)
    rca = CorrelationEngine.classify_root_cause(nodes, edges)
    sig = incident_signature(nodes, rca, incident_events)
    if sig is None:
        return None
    if sig["fault_class"] == "unknown" and fallback_fault_class:
        sig["fault_class"] = fallback_fault_class
    sig["title"] = title
    return sig


def simulated_incidents(n_seeds=15):
    entries = []
    for fault_name in sorted(FAULTS):
        target = FAULTS[fault_name].target_service
        for seed in range(n_seeds):
            events = run_episode(fault_name, seed=seed * 13 + 3)
            onset = failure_onset(events, target)
            if onset is None:
                continue
            incident_events = [e for e in events
                               if onset - HORIZON <= e["_event_ts"] <= onset + 60.0]
            title = f"{fault_name.replace('_', ' ').title()} — seed {seed}"
            entry = _entry_from_events(incident_events, title, fallback_fault_class=fault_name)
            if entry:
                entries.append(entry)
    return entries


def static_incidents():
    with open(os.path.join(EVAL_DIR, "ground_truth.json")) as f:
        gt = json.load(f)
    entries = []
    for scenario, expected in gt.items():
        path = os.path.join(SAMPLE_DIR, scenario)
        with open(path, encoding="utf-8") as f:
            events = [parse_log_line(line) for line in f]
        events = [e for e in events if e]
        for e in events:
            e["_event_ts"] = parse_event_time(e)
        title = scenario.replace(".log", "").replace("_", " ").title()
        entry = _entry_from_events(events, title, fallback_fault_class=expected["root_cause_class"])
        if entry:
            entries.append(entry)
    return entries


def main():
    entries = simulated_incidents() + static_incidents()
    os.makedirs(ARTIFACTS_ROOT, exist_ok=True)
    path = os.path.join(ARTIFACTS_ROOT, "incident_corpus.json")
    with open(path, "w") as f:
        json.dump({"incidents": entries}, f, indent=2)
    print(f"Wrote {len(entries)} incidents to {path}")


if __name__ == "__main__":
    main()
