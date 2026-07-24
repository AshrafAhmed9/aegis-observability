"""
Turns an RCA result into a human-readable incident report, then writes it
out as six files (the "war room" artifacts) an on-call engineer could
actually use.

The report itself is built by whichever of these works, in order:
1. Ask an LLM (Groq) to write it, given the deterministic root cause as
   ground truth -- so the LLM explains a decision the graph already made,
   rather than guessing on its own.
2. If there's no API key, or the call fails for any reason, build a plainer
   report directly from the RCA result -- no LLM involved.
3. If there isn't even a root cause (nothing failed), return a stub.

This is the "graceful degradation" chain: something reasonable always comes
out, even with no ML and no LLM available.
"""

import csv
import io
import json
import os

import rca

PATCH_TEMPLATES = {
    "resource_exhaustion": (
        "- pool = ConnectionPool(max_connections=20)\n"
        "+ pool = ConnectionPool(max_connections=100, timeout=5)\n"
    ),
    "deadlock": (
        "- cursor.execute(update_a); cursor.execute(update_b)\n"
        "+ cursor.execute(update_a, lock_order=['a', 'b'])\n"
        "+ cursor.execute(update_b, lock_order=['a', 'b'])\n"
    ),
    "timeout": (
        "- response = requests.get(url)\n"
        "+ response = requests.get(url, timeout=3, retries=Retry(total=3, backoff_factor=0.5))\n"
    ),
    "failure": (
        "- pass  # no handling\n"
        "+ except ServiceError:\n"
        "+     return fallback_response()\n"
    ),
}


def build_report(rca_result):
    """Returns a plain dict describing the incident. Tries Groq first, then
    falls back to a deterministic report built straight from rca_result."""
    if not rca_result["ranked_root_causes"]:
        return _stub_report()

    if os.environ.get("GROQ_API_KEY"):
        report = _try_groq_report(rca_result)
        if report is not None:
            return report

    return _deterministic_report(rca_result)


def _stub_report():
    return {
        "incident_id": "INC-UNKNOWN",
        "title": "No degraded services detected",
        "root_cause_service": None,
        "root_cause_class": "unknown",
        "confidence": 0.0,
        "hypothesis": "Insufficient data to diagnose an incident.",
        "remediation_steps": [],
    }


def _deterministic_report(rca_result):
    root_cause_service = rca_result["ranked_root_causes"][0]
    node = rca_result["nodes"][root_cause_service]
    root_cause_class = rca.classify_error_type(node["error_classes"])
    symptoms = [s for s in rca_result["ranked_root_causes"] if s != root_cause_service]

    return {
        "incident_id": f"INC-{root_cause_service.upper()}",
        "title": f"{root_cause_service} failure ({root_cause_class})",
        "root_cause_service": root_cause_service,
        "root_cause_class": root_cause_class,
        "confidence": 0.9 if len(rca_result["ranked_root_causes"]) > 1 else 0.6,
        "hypothesis": (
            f"{root_cause_service} logged {', '.join(node['error_classes']) or 'an error'}, "
            f"which propagated to: {', '.join(symptoms) or 'no other services'}."
        ),
        "remediation_steps": [
            f"Investigate {root_cause_service} for {root_cause_class.replace('_', ' ')}.",
            "Confirm downstream services recover once the root cause is fixed.",
        ],
    }


def _try_groq_report(rca_result):
    try:
        from groq import Groq

        root_cause_service = rca_result["ranked_root_causes"][0]
        node = rca_result["nodes"][root_cause_service]
        prompt = (
            "A deterministic root-cause analysis found this ground truth:\n"
            f"Root cause service: {root_cause_service}\n"
            f"Error classes: {list(node['error_classes'])}\n"
            f"Other affected services: {rca_result['ranked_root_causes'][1:]}\n\n"
            "Reply with JSON only: {\"title\": str, \"hypothesis\": str, \"remediation_steps\": [str, ...]}"
        )
        client = Groq(api_key=os.environ["GROQ_API_KEY"])
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        parsed = json.loads(response.choices[0].message.content)

        root_cause_class = rca.classify_error_type(node["error_classes"])
        return {
            "incident_id": f"INC-{root_cause_service.upper()}",
            "title": parsed["title"],
            "root_cause_service": root_cause_service,
            "root_cause_class": root_cause_class,
            "confidence": 0.9,
            "hypothesis": parsed["hypothesis"],
            "remediation_steps": parsed["remediation_steps"],
        }
    except Exception:
        # Any failure here (no network, bad response, missing key) just
        # means we fall back to the deterministic report instead.
        return None


def write_artifacts(report, rca_result, events, output_dir):
    """Writes the six war room files into output_dir. Returns the list of
    file paths written."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = [
        _write_summary(report, rca_result, output_dir),
        _write_timeline(events, output_dir),
        _write_graph(rca_result, output_dir),
        _write_postmortem(report, output_dir),
        _write_csv(events, output_dir),
        _write_patch(report, output_dir),
    ]
    return paths


def _write_summary(report, rca_result, output_dir):
    path = output_dir / "incident_summary.md"
    lines = [
        f"# {report['incident_id']}: {report['title']}",
        "",
        f"**Root cause:** {report['root_cause_service']} ({report['root_cause_class']})",
        f"**Confidence:** {report['confidence']:.0%}",
        f"**Degraded services:** {', '.join(rca_result['degraded_services'])}",
        "",
        "## Hypothesis",
        report["hypothesis"],
    ]
    path.write_text("\n".join(lines) + "\n")
    return path


def _write_timeline(events, output_dir):
    path = output_dir / "incident_timeline.md"
    lines = ["# Incident Timeline", "", "| Time | Service | Severity | Error |", "|---|---|---|---|"]
    for event in sorted(events, key=lambda e: e["timestamp"]):
        lines.append(
            f"| {event['timestamp']:.1f}s | {event['service']} | {event['severity']} | {event.get('error_class') or ''} |"
        )
    path.write_text("\n".join(lines) + "\n")
    return path


def _write_graph(rca_result, output_dir):
    path = output_dir / "incident_graph.md"
    lines = ["# Failure Propagation Graph", "", "```mermaid", "flowchart LR"]
    for cause, effect in sorted(rca_result["edges"]):
        lines.append(f"    {cause} --> {effect}")
    lines.append("```")
    path.write_text("\n".join(lines) + "\n")
    return path


def _write_postmortem(report, output_dir):
    path = output_dir / "postmortem.md"
    lines = [
        f"# Postmortem: {report['incident_id']}",
        "",
        "## What happened",
        report["hypothesis"],
        "",
        "## Action items",
    ]
    lines.extend(f"- {step}" for step in report["remediation_steps"])
    path.write_text("\n".join(lines) + "\n")
    return path


def _write_csv(events, output_dir):
    path = output_dir / "telemetry_db.csv"
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["timestamp", "trace_id", "span_id", "service", "severity", "error_class"])
    writer.writeheader()
    for event in sorted(events, key=lambda e: e["timestamp"]):
        writer.writerow({key: event.get(key, "") for key in writer.fieldnames})
    path.write_text(buffer.getvalue())
    return path


def _write_patch(report, output_dir):
    path = output_dir / "suggested_patch.diff"
    patch = PATCH_TEMPLATES.get(report["root_cause_class"], PATCH_TEMPLATES["failure"])
    path.write_text(patch)
    return path
