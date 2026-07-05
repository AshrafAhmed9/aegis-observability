import time
from collections import deque
from typing import Any, Dict, List, Optional

from .pipeline import IncidentResult
from .predictor import Prediction

SERIES_METRICS = ("latency_ms", "connection_pool_usage", "queue_depth", "active_connections")
SERIES_LEN = 120
INCIDENT_BUFFER = 20


class ServiceState:
    def __init__(self, name: str):
        self.name = name
        self.last_seen: Optional[float] = None
        self.event_count = 0
        self.error_count = 0
        self.last_error_class: Optional[str] = None
        self.status = "OK"
        self.series: Dict[str, deque] = {m: deque(maxlen=SERIES_LEN) for m in SERIES_METRICS}

    def record(self, event: dict) -> None:
        ts = event.get("_event_ts") or time.time()
        self.last_seen = ts
        self.event_count += 1
        severity = event.get("severity", "INFO")
        if severity in ("ERROR", "CRITICAL"):
            self.error_count += 1
            if event.get("err_class"):
                self.last_error_class = event["err_class"]
        self.status = severity if severity in ("WARNING", "ERROR", "CRITICAL") else self.status
        if severity == "INFO" and self.status not in ("ERROR", "CRITICAL"):
            self.status = "OK"
        for metric in SERIES_METRICS:
            value = event.get(metric)
            if value is not None:
                self.series[metric].append([ts, float(value)])

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "last_seen": self.last_seen,
            "event_count": self.event_count,
            "error_count": self.error_count,
            "last_error_class": self.last_error_class,
            "series": {k: list(v) for k, v in self.series.items()},
        }


class LiveState:
    def __init__(self, max_incidents: int = INCIDENT_BUFFER):
        self.services: Dict[str, ServiceState] = {}
        self.incidents: deque = deque(maxlen=max_incidents)
        self.predictions: List[Prediction] = []
        self.last_graph: Dict[str, Any] = {"nodes": [], "edges": []}
        self.totals = {"events": 0, "traces": 0, "incidents": 0, "late": 0}
        self.watermark: Optional[float] = None

    def record_event(self, event: dict) -> None:
        service = event.get("service")
        if not service:
            return
        state = self.services.setdefault(service, ServiceState(service))
        state.record(event)
        self.totals["events"] += 1
        ts = event.get("_event_ts")
        if ts is not None:
            self.watermark = max(self.watermark or ts, ts)

    def record_trace_closed(self) -> None:
        self.totals["traces"] += 1

    def record_late_event(self) -> None:
        self.totals["late"] += 1

    def record_incident(self, result: IncidentResult, ts: float) -> None:
        top_root = result.rca_result["ranked_root_causes"][0] if result.rca_result["ranked_root_causes"] else None
        self.incidents.appendleft({
            "incident_id": result.report.incident_id,
            "title": result.report.title,
            "overall_severity": result.report.overall_severity,
            "root_cause_service": top_root["service"] if top_root else None,
            "root_cause_class": top_root["root_cause_class"] if top_root else None,
            "degraded_services": result.blast_radius["degraded_services"],
            "blast_radius_percentage": result.blast_radius["blast_radius_percentage"],
            "ml_ranking": result.rca_result.get("ml_ranking"),
            "similar_incidents": result.similar_incidents or [],
            "ts": ts,
        })
        self.last_graph = {
            "nodes": result.nodes,
            "edges": result.edges,
            "roles": result.rca_result["roles"],
        }
        self.totals["incidents"] += 1

    def set_predictions(self, predictions: List[Prediction]) -> None:
        self.predictions = predictions

    def snapshot(self) -> dict:
        return {
            "generated_at": time.time(),
            "watermark": self.watermark,
            "services": [s.to_dict() for s in self.services.values()],
            "predictions": [p.to_dict() for p in self.predictions],
            "incidents": list(self.incidents),
            "graph": self.last_graph,
            "totals": dict(self.totals),
        }
