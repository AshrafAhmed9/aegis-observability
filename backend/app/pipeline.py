import time
from dataclasses import dataclass
from typing import Any, Dict, List

from .correlation import CorrelationEngine
from .analyzer import AegisAnalyzer, AegisDiagnosticReport
from .exporter import WarRoomExporter
from .metrics import CORRELATION_DURATION, INCIDENTS_PROCESSED, ROOT_CAUSE_CLASS

@dataclass
class IncidentResult:
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]
    blast_radius: Dict[str, Any]
    rca_result: Dict[str, Any]
    report: AegisDiagnosticReport
    duration: float

def run_incident_pipeline(incident_events: List[Dict[str, Any]], source: str,
                           workspace_root: str, export: bool = True) -> IncidentResult:
    start = time.monotonic()

    nodes, edges = CorrelationEngine.build_propagation_graph(incident_events)
    blast_radius = CorrelationEngine.estimate_blast_radius(incident_events, nodes)
    rca_result = CorrelationEngine.classify_root_cause(nodes, edges)
    report = AegisAnalyzer.analyze(source, incident_events, blast_radius, rca_result)

    if export:
        exporter = WarRoomExporter(workspace_root)
        exporter.export_all(report, incident_events, nodes, edges)

    duration = time.monotonic() - start
    CORRELATION_DURATION.observe(duration)
    INCIDENTS_PROCESSED.inc()
    if rca_result["ranked_root_causes"]:
        ROOT_CAUSE_CLASS.labels(
            root_cause_class=rca_result["ranked_root_causes"][0]["root_cause_class"]
        ).inc()

    return IncidentResult(
        nodes=nodes, edges=edges, blast_radius=blast_radius,
        rca_result=rca_result, report=report, duration=duration,
    )
