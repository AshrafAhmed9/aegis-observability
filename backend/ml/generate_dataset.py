"""Generate labeled training datasets from the seeded simulator.

Episodes run on a fake clock (same technique as eval/run_eval.py) so a
full 300s fault lifecycle generates in milliseconds. Labels are exact
because the simulator knows the fault type, target service, and failure
onset time.

Outputs (gitignored, regenerable):
  ml/data/failure_windows.csv  — one row per (service, 60s window)
  ml/data/rca_candidates.csv   — one row per degraded candidate per incident
"""

import csv
import json
import os
import sys
import time as time_mod
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.simulator as sim_mod
from app.simulator import SimulatedFleet, FAULTS
from app.streaming import parse_event_time
from app.correlation import CorrelationEngine
from app.parser import parse_log_line
from ml.features import (window_features, candidate_features,
                         FAILURE_FEATURES, RCA_FEATURES)

ML_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ML_DIR, "data")
SAMPLE_DIR = os.path.join(os.path.dirname(ML_DIR), "sample_logs")
EVAL_DIR = os.path.join(os.path.dirname(ML_DIR), "eval")

HORIZON = 120.0          # label = failure within this many seconds
WINDOW = 60.0
SAMPLE_EVERY = 5.0       # window stride in event-time seconds
INJECT_AT = 30.0         # fault injection offset into each episode
HEALTHY_DURATION = 360.0

# deadlock_burst has no ramp — no precursor exists, so its pre-failure
# windows stay label=0 (they are genuinely indistinguishable from healthy).
PREDICTABLE_FAULTS = {"redis_connection_leak", "queue_backlog"}


def run_episode(fault_name, seed):
    """Run one full episode on a fake clock; returns sorted event list."""
    original_datetime = sim_mod.datetime
    fake_now = [datetime(2026, 1, 1, tzinfo=timezone.utc)]

    class FakeDateTime(original_datetime):
        @classmethod
        def now(cls, tz=None):
            return fake_now[0]

    sim_mod.datetime = FakeDateTime
    try:
        events = []
        fleet = SimulatedFleet(emit=events.append, seed=seed)
        if fault_name:
            fault_cls = FAULTS[fault_name]
            total = INJECT_AT + fault_cls.ramp_seconds + fault_cls.failure_seconds + \
                    fault_cls.recovery_seconds + 10.0
        else:
            total = HEALTHY_DURATION

        step = fleet.TICK_SECONDS
        real_start = time_mod.monotonic()
        injected = False
        t = 0.0
        while t <= total:
            if fault_name and not injected and t >= INJECT_AT:
                fleet.inject(fault_name)
                injected = True
            if injected:
                fleet._fault_started_at = real_start - (t - INJECT_AT)
                # tick() clears finished faults; freeze the reference we need
            fleet.tick()
            fake_now[0] += timedelta(seconds=step)
            t += step
    finally:
        sim_mod.datetime = original_datetime

    for e in events:
        e["_event_ts"] = parse_event_time(e)
    events.sort(key=lambda e: e["_event_ts"] or 0.0)
    return events


def failure_onset(events, target_service):
    for e in events:
        if e.get("service") == target_service and e.get("severity") in ("ERROR", "CRITICAL"):
            return e["_event_ts"]
    return None


def episode_failure_rows(events, fault_name, seed, episode_id):
    """Windowed feature rows with leakage-safe labels."""
    target = FAULTS[fault_name].target_service if fault_name else None
    onset = failure_onset(events, target) if target else None

    by_service = {}
    for e in events:
        svc = e.get("service")
        if svc:
            by_service.setdefault(svc, []).append(e)

    t_start = events[0]["_event_ts"]
    t_end = events[-1]["_event_ts"]
    # For fault episodes, no window may end at/after failure onset:
    # everything from onset onward is "incident in progress", not a
    # prediction sample.
    cutoff = onset if onset is not None else t_end

    rows = []
    window_end = t_start + WINDOW
    while window_end < cutoff:
        for svc, svc_events in by_service.items():
            feats = window_features(svc_events, window_end, WINDOW)
            if feats is None:
                continue
            label = 0
            seconds_to_onset = -1.0
            if (target and svc == target and fault_name in PREDICTABLE_FAULTS
                    and onset is not None and 0 < onset - window_end <= HORIZON):
                label = 1
                seconds_to_onset = onset - window_end
            row = {"episode_id": episode_id, "seed": seed,
                   "fault": fault_name or "healthy", "service": svc,
                   "window_end": round(window_end - t_start, 1), "label": label,
                   "seconds_to_onset": round(seconds_to_onset, 1)}
            row.update({k: round(feats[k], 6) for k in FAILURE_FEATURES})
            rows.append(row)
        window_end += SAMPLE_EVERY
    return rows


def episode_rca_rows(events, fault_name, seed, episode_id):
    """One row per degraded candidate service for one incident."""
    target = FAULTS[fault_name].target_service
    onset = failure_onset(events, target)
    if onset is None:
        return []
    incident_events = [e for e in events
                       if onset - HORIZON <= e["_event_ts"] <= onset + 60.0]
    nodes, edges = CorrelationEngine.build_propagation_graph(incident_events)
    rca = CorrelationEngine.classify_root_cause(nodes, edges)
    kahn_pick = rca["ranked_root_causes"][0]["service"] if rca["ranked_root_causes"] else None
    kahn_correct = 1 if kahn_pick == target else 0
    degraded = [n for n in nodes
                if n["max_severity"] in ("ERROR", "CRITICAL")]
    rows = []
    for node in degraded:
        feats = candidate_features(node, nodes, edges, rca["topo_order"])
        row = {"episode_id": episode_id, "seed": seed, "fault": fault_name,
               "service": node["service_name"],
               "label": 1 if node["service_name"] == target else 0,
               "kahn_top1_correct": kahn_correct}
        row.update({k: round(feats[k], 6) for k in RCA_FEATURES})
        rows.append(row)
    return rows


def static_scenario_rca_rows():
    """The 3 curated log scenarios as extra labeled RCA incidents."""
    with open(os.path.join(EVAL_DIR, "ground_truth.json")) as f:
        gt = json.load(f)
    rows = []
    for scenario, expected in gt.items():
        path = os.path.join(SAMPLE_DIR, scenario)
        with open(path, encoding="utf-8") as f:
            events = [parse_log_line(line) for line in f]
        events = [e for e in events if e]
        for e in events:
            e["_event_ts"] = parse_event_time(e)
        nodes, edges = CorrelationEngine.build_propagation_graph(events)
        rca = CorrelationEngine.classify_root_cause(nodes, edges)
        kahn_pick = rca["ranked_root_causes"][0]["service"] if rca["ranked_root_causes"] else None
        kahn_correct = 1 if kahn_pick == expected["root_cause_service"] else 0
        degraded = [n for n in nodes if n["max_severity"] in ("ERROR", "CRITICAL")]
        for node in degraded:
            feats = candidate_features(node, nodes, edges, rca["topo_order"])
            row = {"episode_id": f"static-{scenario}", "seed": -1,
                   "fault": scenario, "service": node["service_name"],
                   "label": 1 if node["service_name"] == expected["root_cause_service"] else 0,
                   "kahn_top1_correct": kahn_correct}
            row.update({k: round(feats[k], 6) for k in RCA_FEATURES})
            rows.append(row)
    return rows


def main(n_seeds=25, seed_base=1, seed_stride=7):
    os.makedirs(DATA_DIR, exist_ok=True)
    failure_rows, rca_rows = [], []
    episode_id = 0

    fault_names = [None] + sorted(FAULTS)
    for fault_name in fault_names:
        for seed in range(n_seeds):
            episode_id += 1
            events = run_episode(fault_name, seed=seed_base + seed * seed_stride)
            failure_rows.extend(episode_failure_rows(events, fault_name, seed, episode_id))
            if fault_name:
                rca_rows.extend(episode_rca_rows(events, fault_name, seed, episode_id))
            label = fault_name or "healthy"
            print(f"episode {episode_id:3d} [{label:>22s} seed={seed:2d}] "
                  f"events={len(events)}")

    rca_rows.extend(static_scenario_rca_rows())

    fpath = os.path.join(DATA_DIR, "failure_windows.csv")
    with open(fpath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["episode_id", "seed", "fault", "service",
                                                "window_end", "label", "seconds_to_onset"] + FAILURE_FEATURES)
        writer.writeheader()
        writer.writerows(failure_rows)

    rpath = os.path.join(DATA_DIR, "rca_candidates.csv")
    with open(rpath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["episode_id", "seed", "fault", "service",
                                                "label", "kahn_top1_correct"] + RCA_FEATURES)
        writer.writeheader()
        writer.writerows(rca_rows)

    positives = sum(r["label"] for r in failure_rows)
    print(f"\nfailure_windows.csv: {len(failure_rows)} rows, "
          f"{positives} positive ({100 * positives / max(1, len(failure_rows)):.1f}%)")
    rpos = sum(r["label"] for r in rca_rows)
    print(f"rca_candidates.csv:  {len(rca_rows)} rows, {rpos} positive")


if __name__ == "__main__":
    main()
