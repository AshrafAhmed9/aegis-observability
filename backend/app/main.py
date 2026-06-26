from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import os
import time
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from .parser import parse_log_line
from .correlation import CorrelationEngine
from .analyzer import AegisAnalyzer
from .jetro_service import JetroService
from .streaming import StreamingCorrelator, IncidentAssembler
from .metrics import (
    EVENTS_INGESTED, TRACES_EMITTED, INCIDENTS_PROCESSED,
    LATE_EVENTS, OPEN_TRACES, CORRELATION_DURATION, ROOT_CAUSE_CLASS,
)

WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = FastAPI(
    title="Aegis AI-Native Incident Correlation & Observability Platform",
    version="2.0.0",
    description="Deterministic telemetry correlation engine with streaming windowed correlation and topological RCA."
)

class IngestRequest(BaseModel):
    log_filename: str = "redis_retry_storm.log"

@app.get("/")
def read_root():
    return {
        "status": "ONLINE",
        "platform": "Aegis",
        "version": "2.0.0",
        "description": "AI-Native Incident Correlation & Observability Platform"
    }

@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/ingest")
def ingest_log_file(request: IngestRequest):
    sample_logs_dir = os.path.join(WORKSPACE_ROOT, "backend", "sample_logs")
    log_path = os.path.join(sample_logs_dir, request.log_filename)

    if not os.path.exists(log_path):
        raise HTTPException(
            status_code=404,
            detail=f"Log file '{request.log_filename}' not found in sample directory: {sample_logs_dir}"
        )

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        parsed_events = []
        for line in lines:
            parsed = parse_log_line(line)
            if parsed:
                parsed_events.append(parsed)

        if not parsed_events:
            raise HTTPException(status_code=400, detail="No valid structured telemetry events parsed from log file.")

        start = time.monotonic()

        correlator = StreamingCorrelator(wall_idle=float("inf"), grace=120.0)
        for event in parsed_events:
            correlator.ingest(event)
        emitted_traces = correlator.flush_all()

        if not emitted_traces:
            raise HTTPException(status_code=400, detail="No valid trace IDs found in log events.")

        assembler = IncidentAssembler()
        for trace in emitted_traces:
            assembler.add(trace)
        incident_events = assembler.flush()

        if not incident_events:
            raise HTTPException(status_code=400, detail="No incident assembled from traces.")

        nodes, edges = CorrelationEngine.build_propagation_graph(incident_events)
        blast_radius = CorrelationEngine.estimate_blast_radius(incident_events, nodes)
        rca_result = CorrelationEngine.classify_root_cause(nodes, edges)

        report = AegisAnalyzer.analyze(request.log_filename, incident_events, blast_radius, rca_result)

        jetro = JetroService(WORKSPACE_ROOT)
        jetro.export_all(report, incident_events, nodes, edges)

        duration = time.monotonic() - start
        EVENTS_INGESTED.inc(len(parsed_events))
        TRACES_EMITTED.inc(len(emitted_traces))
        INCIDENTS_PROCESSED.inc()
        LATE_EVENTS.inc(correlator.late_count)
        CORRELATION_DURATION.observe(duration)
        if rca_result["ranked_root_causes"]:
            ROOT_CAUSE_CLASS.labels(
                root_cause_class=rca_result["ranked_root_causes"][0]["root_cause_class"]
            ).inc()

        return {
            "status": "SUCCESS",
            "message": f"Successfully ingested {len(lines)} telemetry rows across {len(emitted_traces)} traces.",
            "correlation": {
                "traces_correlated": len(emitted_traces),
                "incident_events": len(incident_events),
                "late_events": correlator.late_count,
            },
            "root_cause_analysis": {
                "ranked_root_causes": rca_result["ranked_root_causes"],
                "roles": rca_result["roles"],
                "cycle": rca_result["cycle"],
                "edge_basis": rca_result["edge_basis"],
            },
            "incident_report": {
                "incident_id": report.incident_id,
                "title": report.title,
                "overall_severity": report.overall_severity,
                "degraded_services": blast_radius["degraded_services"],
                "estimated_affected_requests": blast_radius["estimated_affected_requests"],
                "blast_radius_percentage": f"{blast_radius['blast_radius_percentage']:.1f}%",
                "hypotheses_count": len(report.hypotheses),
                "primary_mitigation_file": report.primary_remediation.file_path
            },
            "exported_files": [
                "active_war_room/incident_summary.md",
                "active_war_room/incident_timeline.md",
                "active_war_room/incident_graph.md",
                "active_war_room/postmortem.md",
                "active_war_room/telemetry_db.csv",
                "active_war_room/suggested_patch.diff"
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Telemetry pipeline failure: {str(e)}\n{traceback.format_exc()}"
        )

@app.get("/scenarios")
def list_scenarios():
    return {
        "scenarios": [
            {
                "log_filename": "redis_retry_storm.log",
                "title": "Redis Connection Pool Exhaustion & Celery Task Retry Storm Cascade",
                "domain": "Distributed Queuing, Cache Stampede, Backpressure"
            },
            {
                "log_filename": "pg_deadlock.log",
                "title": "PostgreSQL Row Lock Deadlock under Concurrent Transactions",
                "domain": "Database Transactions, Race Conditions, Locks"
            },
            {
                "log_filename": "cache_stampede.log",
                "title": "Cache Miss Stampede & Primary Database Starvation under Concurrent Read Peak",
                "domain": "Caching, High-Throughput I/O, Rate Limiting"
            }
        ]
    }
