import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.parser import parse_log_line
from app.streaming import StreamingCorrelator, IncidentAssembler
from app.correlation import CorrelationEngine
from app.analyzer import AegisAnalyzer, AegisDiagnosticReport

EVAL_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(EVAL_DIR)
SAMPLE_DIR = os.path.join(BACKEND_DIR, "sample_logs")


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

        # Check 1: deterministic root cause match
        top_root = rca["ranked_root_causes"][0] if rca["ranked_root_causes"] else None
        root_match = top_root and top_root["service"] == expected["root_cause_service"]
        class_match = top_root and top_root["root_cause_class"] == expected["root_cause_class"]

        # Check 2: schema validity (report is a valid AegisDiagnosticReport)
        schema_valid = isinstance(report, AegisDiagnosticReport)

        # Check 3: degraded services match
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

    # Scorecard
    total = len(results)
    root_score = sum(1 for r in results if r["root_match"])
    class_score = sum(1 for r in results if r["class_match"])
    schema_score = sum(1 for r in results if r["schema_valid"])
    degraded_score = sum(1 for r in results if r["degraded_match"])

    print(f"\n{'='*60}")
    print(f"SCORECARD")
    print(f"{'='*60}")
    print(f"  Root cause top-1 match:  {root_score}/{total}")
    print(f"  Root cause class match:  {class_score}/{total}")
    print(f"  Schema validity:         {schema_score}/{total}")
    print(f"  Degraded services match: {degraded_score}/{total}")
    print(f"{'='*60}")

    # Write scorecard
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
    print(f"\nScorecard written to {scorecard_path}")


if __name__ == "__main__":
    evaluate()
