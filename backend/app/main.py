from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
from .parser import parse_log_line
from .correlation import CorrelationEngine
from .analyzer import AegisAnalyzer
from .jetro_service import JetroService

# Get absolute workspace root path of Aegis
WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = FastAPI(
    title="Aegis AI-Native Incident Correlation & Observability Platform",
    version="1.0.0",
    description="Deterministic telemetry correlation engine and structured SRE diagnostic whiteboard generator."
)

class IngestRequest(BaseModel):
    log_filename: str = "redis_retry_storm.log"

@app.get("/")
def read_root():
    return {
        "status": "ONLINE",
        "platform": "Aegis",
        "description": "AI-Native Incident Correlation & Observability Platform"
    }

@app.post("/ingest")
def ingest_log_file(request: IngestRequest):
    """
    Ingests raw distributed log telemetry, correlates events, maps propagation graphs,
    and publishes the 'Incident War Room' spatial cockpit to the Jetro workspace.
    """
    sample_logs_dir = os.path.join(WORKSPACE_ROOT, "backend", "sample_logs")
    log_path = os.path.join(sample_logs_dir, request.log_filename)
    
    if not os.path.exists(log_path):
        raise HTTPException(
            status_code=404, 
            detail=f"Log file '{request.log_filename}' not found in sample directory: {sample_logs_dir}"
        )
        
    try:
        # 1. Parse raw log lines
        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            
        parsed_events = []
        for line in lines:
            parsed = parse_log_line(line)
            if parsed:
                parsed_events.append(parsed)
                
        if not parsed_events:
            raise HTTPException(status_code=400, detail="No valid structured telemetry events parsed from log file.")
            
        # 2. Correlate events deterministically via Telemetry Correlation Engine
        correlated_traces = CorrelationEngine.correlate(parsed_events)
        if not correlated_traces:
            raise HTTPException(status_code=400, detail="No valid trace IDs found in log events.")
        # Pull the primary incident trace (usually the longest or containing errors)
        # For our pre-packaged logs, they correspond to a single correlated trace.
        primary_trace_id = list(correlated_traces.keys())[0]
        trace_events = correlated_traces[primary_trace_id]
        
        # 3. Model Failure Propagation Graph
        nodes, edges = CorrelationEngine.build_propagation_graph(trace_events)
        
        # 4. Estimate Blast Radius Metrics
        blast_radius = CorrelationEngine.estimate_blast_radius(trace_events, nodes)
        
        # 5. Run Pydantic AI Diagnostic Augmentor
        report = AegisAnalyzer.analyze(request.log_filename, trace_events, blast_radius)
        
        # 6. Publish / Export SRE War Room whiteboard assets to Jetro Workspace
        jetro = JetroService(WORKSPACE_ROOT)
        jetro.export_all(report, trace_events, nodes, edges)
        
        return {
            "status": "SUCCESS",
            "message": f"Successfully ingested {len(lines)} telemetry rows.",
            "trace_id": primary_trace_id,
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
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Telemetry pipeline failure: {str(e)}\n{traceback.format_exc()}"
        )

@app.get("/scenarios")
def list_scenarios():
    """
    Lists the available high-fidelity failure scenarios for rapid demo testing.
    """
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
