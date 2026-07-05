import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .correlation import CorrelationEngine
from .analyzer import AegisAnalyzer, AegisDiagnosticReport
from .exporter import WarRoomExporter
from . import rca_ranker
from . import incident_memory
from .metrics import CORRELATION_DURATION, INCIDENTS_PROCESSED, ROOT_CAUSE_CLASS

@dataclass
class IncidentResult:
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]
    blast_radius: Dict[str, Any]
    rca_result: Dict[str, Any]
    report: AegisDiagnosticReport
    duration: float
    similar_incidents: Optional[List[Dict[str, Any]]] = None

def run_incident_pipeline(incident_events: List[Dict[str, Any]], source: str,
                           workspace_root: str, export: bool = True) -> IncidentResult:
    start = time.monotonic()

    nodes, edges = CorrelationEngine.build_propagation_graph(incident_events)
    blast_radius = CorrelationEngine.estimate_blast_radius(incident_events, nodes)
    rca_result = CorrelationEngine.classify_root_cause(nodes, edges)
    ml_ranking = rca_ranker.score_candidates(nodes, edges, rca_result["topo_order"])
    if ml_ranking is not None:
        rca_result["ml_ranking"] = ml_ranking
    report = AegisAnalyzer.analyze(source, incident_events, blast_radius, rca_result)

    similar_incidents = None
    memory = incident_memory.get_memory()
    if memory is not None:
        from ml.features import incident_signature
        sig = incident_signature(nodes, rca_result, incident_events)
        if sig is not None:
            similar_incidents = memory.similar(sig["signature"], top_k=3)
            memory.add({
                "title": f"{report.title} ({time.strftime('%H:%M:%S')})",
                "fault_class": sig["fault_class"],
                "signature": sig["signature"],
            })

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
        similar_incidents=similar_incidents,
    )
