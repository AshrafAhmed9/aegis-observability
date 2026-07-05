import json
import os
import sys
import time as time_mod
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.parser import parse_log_line
from app.streaming import StreamingCorrelator, IncidentAssembler, parse_event_time
from app.correlation import CorrelationEngine
from app.analyzer import AegisAnalyzer, AegisDiagnosticReport
from app.simulator import SimulatedFleet, FAULTS
from app.predictor import FailurePredictor, TREND_BREACH
from app.ml_predictor import MLFailureDetector, ML_RISK
import app.simulator as sim_mod

EVAL_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(EVAL_DIR)
SAMPLE_DIR = os.path.join(BACKEND_DIR, "sample_logs")

MIN_LEAD_SECONDS = 60.0


def load_ground_truth():
    with open(os.path.join(EVAL_DIR, "ground_truth.json")) as f:
        return json.load(f)


def run_pipeline(log_filename):
    log_path = os.path.join(SAMPLE_DIR, log_filename)
    with open(log_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    parsed = [parse_log_line(line) for line in lines]
    parsed = [e for e in parsed if e]

    correlator = StreamingCorrelator(wall_idle=float("inf"), grace=120.0)
    for event in parsed:
        correlator.ingest(event)
    emitted = correlator.flush_all()

    assembler = IncidentAssembler()
    for trace in emitted:
        assembler.add(trace)
    incident_events = assembler.flush()

    nodes, edges = CorrelationEngine.build_propagation_graph(incident_events)
    blast_radius = CorrelationEngine.estimate_blast_radius(incident_events, nodes)
    rca_result = CorrelationEngine.classify_root_cause(nodes, edges)
    report = AegisAnalyzer.analyze(log_filename, incident_events, blast_radius, rca_result)

    return {
        "rca_result": rca_result,
        "blast_radius": blast_radius,
        "report": report,
        "traces": len(emitted),
        "events": len(incident_events),
        "late": correlator.late_count,
    }


def evaluate_prediction(fault_name="redis_connection_leak", min_lead_seconds=MIN_LEAD_SECONDS, seed=42):
    """Drive the simulator through a full fault ramp+failure deterministically
    (no real sleeps, no live server) and measure how many seconds before the
    actual breach the trend predictor raised its first warning. Uses the same
    fake-clock technique as the simulator's own regression test."""
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
        fleet.inject(fault_name)
        fault = fleet._fault

        total_seconds = fault.ramp_seconds + fault.failure_seconds + 5.0
        step = fleet.TICK_SECONDS
        real_start = time_mod.monotonic()
        t = 0.0
        while t <= total_seconds:
            fleet._fault_started_at = real_start - t
            fleet.tick()
            fake_now[0] += timedelta(seconds=step)
            t += step
    finally:
        sim_mod.datetime = original_datetime

    for event in events:
        event["_event_ts"] = parse_event_time(event)
    events.sort(key=lambda e: e["_event_ts"] or 0.0)

    predictor = FailurePredictor()
    first_pred_ts = None
    for event in events:
        preds = predictor.observe(event)
        for p in preds:
            if p.kind == TREND_BREACH and p.service == fault.target_service and first_pred_ts is None:
                first_pred_ts = p.predicted_at

    first_error_ts = None
    for event in events:
        if event.get("service") == fault.target_service and event.get("severity") in ("ERROR", "CRITICAL"):
            first_error_ts = event["_event_ts"]
            break

    if first_pred_ts is None or first_error_ts is None:
        return {"fault": fault_name, "lead_seconds": None, "passed": False}

    lead = first_error_ts - first_pred_ts
    return {"fault": fault_name, "lead_seconds": lead, "passed": lead >= min_lead_seconds}


def _run_fault_episode_events(fault_name, seed):
    """Drive one full fault episode on a fake clock; returns sorted events
    with _event_ts set. Same technique as evaluate_prediction()."""
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
        fleet.inject(fault_name)
        fault = fleet._fault

        total_seconds = fault.ramp_seconds + fault.failure_seconds + 5.0
        step = fleet.TICK_SECONDS
        real_start = time_mod.monotonic()
        t = 0.0
        while t <= total_seconds:
            fleet._fault_started_at = real_start - t
            fleet.tick()
            fake_now[0] += timedelta(seconds=step)
            t += step
    finally:
        sim_mod.datetime = original_datetime

    for event in events:
        event["_event_ts"] = parse_event_time(event)
    events.sort(key=lambda e: e["_event_ts"] or 0.0)
    return events, fleet._fault if hasattr(fleet, "_fault") else None


def evaluate_stat_vs_ml(fault_name="redis_connection_leak", seeds=(9001, 9002, 9003)):
    """True apples-to-apples comparison: runs the ACTUAL FailurePredictor and
    MLFailureDetector runtime classes (not reconstructed offline metrics)
    side-by-side over identical, genuinely unseen-seed episodes (training
    used seeds 1,8,...,169; these are far outside that set)."""
    ml = MLFailureDetector.load()
    if ml is None:
        return {"fault": fault_name, "available": False}

    target = FAULTS[fault_name].target_service
    stat_leads, ml_leads = [], []
    for seed in seeds:
        events, _ = _run_fault_episode_events(fault_name, seed)

        stat = FailurePredictor()
        ml_run = MLFailureDetector.load()  # fresh internal buffers per episode
        first_stat_ts, first_ml_ts, first_error_ts = None, None, None

        for event in events:
            for p in stat.observe(event):
                if p.kind == TREND_BREACH and p.service == target and first_stat_ts is None:
                    first_stat_ts = p.predicted_at
            for p in ml_run.observe(event):
                if p.kind == ML_RISK and p.service == target and first_ml_ts is None:
                    first_ml_ts = p.predicted_at
            if (event.get("service") == target and event.get("severity") in ("ERROR", "CRITICAL")
                    and first_error_ts is None):
                first_error_ts = event["_event_ts"]

        if first_error_ts is not None:
            if first_stat_ts is not None:
                stat_leads.append(first_error_ts - first_stat_ts)
            if first_ml_ts is not None:
                ml_leads.append(first_error_ts - first_ml_ts)

    def _median(values):
        if not values:
            return None
        s = sorted(values)
        n = len(s)
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    return {
        "fault": fault_name, "available": True, "n_episodes": len(seeds),
        "stat_median_lead": _median(stat_leads), "stat_catches": len(stat_leads),
        "ml_median_lead": _median(ml_leads), "ml_catches": len(ml_leads),
    }


def evaluate():
    gt = load_ground_truth()
    results = []

    for scenario, expected in gt.items():
        print(f"\n{'='*60}")
        print(f"SCENARIO: {scenario}")
        print(f"{'='*60}")

        pipeline = run_pipeline(scenario)
        rca = pipeline["rca_result"]
        report = pipeline["report"]

        top_root = rca["ranked_root_causes"][0] if rca["ranked_root_causes"] else None
        root_match = top_root and top_root["service"] == expected["root_cause_service"]
        class_match = top_root and top_root["root_cause_class"] == expected["root_cause_class"]

        schema_valid = isinstance(report, AegisDiagnosticReport)

        actual_degraded = set(pipeline["blast_radius"]["degraded_services"])
        expected_degraded = set(expected["expected_degraded"])
        degraded_match = expected_degraded.issubset(actual_degraded)

        result = {
            "scenario": scenario,
            "root_match": root_match,
            "class_match": class_match,
            "schema_valid": schema_valid,
            "degraded_match": degraded_match,
            "top_root": top_root["service"] if top_root else "NONE",
            "expected_root": expected["root_cause_service"],
            "edge_basis": rca["edge_basis"],
            "traces": pipeline["traces"],
            "events": pipeline["events"],
        }
        results.append(result)

        status = "PASS" if root_match else "FAIL"
        print(f"  Root cause:  {top_root['service'] if top_root else 'NONE'} (expected: {expected['root_cause_service']}) [{status}]")
        print(f"  Class:       {top_root['root_cause_class'] if top_root else 'NONE'} (expected: {expected['root_cause_class']}) [{'PASS' if class_match else 'FAIL'}]")
        print(f"  Schema:      [{'PASS' if schema_valid else 'FAIL'}]")
        print(f"  Degraded:    {sorted(actual_degraded)} [{'PASS' if degraded_match else 'FAIL'}]")
        print(f"  Edge basis:  {rca['edge_basis']}")
        print(f"  Traces/Events: {pipeline['traces']}/{pipeline['events']}")

    total = len(results)
    root_score = sum(1 for r in results if r["root_match"])
    class_score = sum(1 for r in results if r["class_match"])
    schema_score = sum(1 for r in results if r["schema_valid"])
    degraded_score = sum(1 for r in results if r["degraded_match"])

    print(f"\n{'='*60}")
    print(f"PREDICTION LEAD TIME")
    print(f"{'='*60}")
    pred_result = evaluate_prediction()
    if pred_result["lead_seconds"] is not None:
        pred_status = "PASS" if pred_result["passed"] else "FAIL"
        print(f"  {pred_result['fault']}: predicted {pred_result['lead_seconds']:.0f}s before breach "
              f"(threshold {MIN_LEAD_SECONDS:.0f}s) [{pred_status}]")
    else:
        print(f"  {pred_result['fault']}: no prediction fired before breach [FAIL]")

    print(f"\n{'='*60}")
    print(f"STAT vs ML — apples-to-apples (unseen seeds, real runtime classes)")
    print(f"{'='*60}")
    cmp_result = evaluate_stat_vs_ml()
    if not cmp_result["available"]:
        print(f"  ML model not available (no artifact/deps) — skipped")
    else:
        print(f"  {cmp_result['fault']} over {cmp_result['n_episodes']} unseen-seed episodes:")
        print(f"    STAT: caught {cmp_result['stat_catches']}/{cmp_result['n_episodes']}, "
              f"median lead {cmp_result['stat_median_lead']:.0f}s"
              if cmp_result["stat_median_lead"] is not None else
              f"    STAT: caught {cmp_result['stat_catches']}/{cmp_result['n_episodes']}")
        print(f"    ML:   caught {cmp_result['ml_catches']}/{cmp_result['n_episodes']}, "
              f"median lead {cmp_result['ml_median_lead']:.0f}s"
              if cmp_result["ml_median_lead"] is not None else
              f"    ML:   caught {cmp_result['ml_catches']}/{cmp_result['n_episodes']}")

    print(f"\n{'='*60}")
    print(f"SCORECARD")
    print(f"{'='*60}")
    print(f"  Root cause top-1 match:  {root_score}/{total}")
    print(f"  Root cause class match:  {class_score}/{total}")
    print(f"  Schema validity:         {schema_score}/{total}")
    print(f"  Degraded services match: {degraded_score}/{total}")
    print(f"{'='*60}")

    scorecard_path = os.path.join(EVAL_DIR, "scorecard.md")
    with open(scorecard_path, "w") as f:
        f.write("# Aegis RCA Eval Scorecard\n\n")
        f.write(f"| Metric | Score |\n|--------|-------|\n")
        f.write(f"| Root cause top-1 match | {root_score}/{total} |\n")
        f.write(f"| Root cause class match | {class_score}/{total} |\n")
        f.write(f"| Schema validity | {schema_score}/{total} |\n")
        f.write(f"| Degraded services match | {degraded_score}/{total} |\n\n")
        for r in results:
            f.write(f"## {r['scenario']}\n")
            f.write(f"- Root: {r['top_root']} (expected: {r['expected_root']}) {'PASS' if r['root_match'] else 'FAIL'}\n")
            f.write(f"- Class: {'PASS' if r['class_match'] else 'FAIL'}\n")
            f.write(f"- Edge basis: {r['edge_basis']}\n")
            f.write(f"- Traces/Events: {r['traces']}/{r['events']}\n\n")

        f.write("## Prediction Lead Time\n")
        if pred_result["lead_seconds"] is not None:
            f.write(f"- {pred_result['fault']}: {pred_result['lead_seconds']:.0f}s lead "
                    f"({'PASS' if pred_result['passed'] else 'FAIL'}, threshold {MIN_LEAD_SECONDS:.0f}s)\n\n")
        else:
            f.write(f"- {pred_result['fault']}: no prediction fired (FAIL)\n\n")

        f.write("## STAT vs ML (unseen seeds, real runtime classes)\n")
        if not cmp_result["available"]:
            f.write("- ML model not available (no artifact/deps)\n\n")
        else:
            f.write(f"- Fault: {cmp_result['fault']}, episodes: {cmp_result['n_episodes']}\n")
            f.write(f"- STAT: caught {cmp_result['stat_catches']}/{cmp_result['n_episodes']}"
                    + (f", median lead {cmp_result['stat_median_lead']:.0f}s\n"
                       if cmp_result["stat_median_lead"] is not None else "\n"))
            f.write(f"- ML: caught {cmp_result['ml_catches']}/{cmp_result['n_episodes']}"
                    + (f", median lead {cmp_result['ml_median_lead']:.0f}s\n\n"
                       if cmp_result["ml_median_lead"] is not None else "\n\n"))
    print(f"\nScorecard written to {scorecard_path}")


if __name__ == "__main__":
    evaluate()
