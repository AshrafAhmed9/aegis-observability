import asyncio
import json
import os
import time
from contextlib import asynccontextmanager, suppress
from .simulator import SimulatedFleet
from fastapi.staticfiles import StaticFiles

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, ConfigDict
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from .parser import parse_log_line
from .streaming import StreamingCorrelator, IncidentAssembler
from .predictor import FailurePredictor
from .live_state import LiveState
from .pipeline import run_incident_pipeline
from . import infra
from .metrics import (
    EVENTS_INGESTED, TRACES_EMITTED, LATE_EVENTS, OPEN_TRACES,
    PREDICTIONS_ACTIVE, PREDICTED_BREACH_ETA,
)

WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TICK_INTERVAL = 2.0

correlator = StreamingCorrelator(wall_idle=10.0, grace=5.0)
# Short window: unlike batch /ingest (one log file, force-flushed at the end),
# the live stream runs continuously, so a long window would bury a real
# failure under minutes of unrelated healthy traffic before it ever flushes.
assembler = IncidentAssembler(window=20.0)
predictor = FailurePredictor()
live_state = LiveState()


def _update_prediction_gauges():
    active = predictor.active()
    live_state.set_predictions(active)
    PREDICTIONS_ACTIVE.set(len(active))
    for pred in active:
        if pred.eta_seconds is not None:
            PREDICTED_BREACH_ETA.labels(service=pred.service, metric=pred.metric).set(pred.eta_seconds)


def ingest_live_event(event: dict) -> dict:
    closed = correlator.ingest(event)
    predictor.observe(event)
    live_state.record_event(event)
    _update_prediction_gauges()
    _handle_closed_traces(closed)
    return {"accepted": True, "open_traces": correlator.open_trace_count,
            "active_predictions": len(predictor.active())}


def _has_degraded_signal(events) -> bool:
    return any(e.get("severity") in ("ERROR", "CRITICAL") for e in events)


def _handle_closed_traces(closed_traces):
    for trace in closed_traces:
        live_state.record_trace_closed()
        incident_events = assembler.add(trace)
        if incident_events and _has_degraded_signal(incident_events):
            _process_incident(incident_events, "live-stream")


def _process_incident(incident_events, source):
    result = run_incident_pipeline(incident_events, source, WORKSPACE_ROOT)
    live_state.record_incident(result, time.time())


simulator = SimulatedFleet(emit=ingest_live_event)

KAFKA_BROKER = os.environ.get("AEGIS_KAFKA_ADDR", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry.raw")

_replay_task = None
_replay_status = {"state": "idle", "scenario": None, "sent": 0, "total": 0, "error": None}


async def _run_kafka_replay(events, rate):
    from aiokafka import AIOKafkaProducer
    from aiokafka.errors import KafkaConnectionError

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    try:
        await asyncio.wait_for(producer.start(), timeout=5)
        _replay_status.update(state="running", error=None)
        delay = 1.0 / rate if rate > 0 else 0
        for event in events:
            safe = {k: v for k, v in event.items() if not k.startswith("_")}
            await producer.send_and_wait(KAFKA_TOPIC, value=safe, key=event.get("trace_id", "unknown"))
            _replay_status["sent"] += 1
            if delay:
                await asyncio.sleep(delay)
        _replay_status["state"] = "done"
    except (KafkaConnectionError, asyncio.TimeoutError, OSError) as e:
        _replay_status.update(state="error", error=f"broker unreachable: {e}")
    except asyncio.CancelledError:
        _replay_status["state"] = "cancelled"
        raise
    finally:
        await producer.stop()


async def _background_tick():
    while True:
        await asyncio.sleep(TICK_INTERVAL)
        closed = correlator.tick()
        LATE_EVENTS.inc(correlator.late_count)
        OPEN_TRACES.set(correlator.open_trace_count)
        _handle_closed_traces(closed)
        _update_prediction_gauges()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_background_tick())
    yield
    task.cancel()
    if _replay_task is not None and not _replay_task.done():
        _replay_task.cancel()
        with suppress(asyncio.CancelledError):
            await _replay_task


app = FastAPI(
    title="Aegis AI-Native Incident Correlation & Observability Platform",
    version="3.0.0",
    description="Streaming correlation, topological RCA, and predictive failure detection.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class IngestRequest(BaseModel):
    log_filename: str = "redis_retry_storm.log"


class EventRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    raw_line: str | None = None


class SimulatorInjectRequest(BaseModel):
    fault: str


class KafkaReplayRequest(BaseModel):
    scenario: str = "redis_retry_storm.log"
    rate: float = 5.0


@app.get("/health")
def health():
    return {
        "status": "ONLINE",
        "platform": "Aegis",
        "version": "3.0.0",
        "description": "AI-Native Incident Correlation & Observability Platform",
    }


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/events")
def post_event(request: EventRequest):
    payload = request.model_dump(exclude_none=True)
    raw_line = payload.pop("raw_line", None)
    if raw_line:
        event = parse_log_line(raw_line)
        if not event:
            raise HTTPException(status_code=400, detail="Could not parse raw_line as a telemetry event.")
    else:
        event = payload
    if not event.get("trace_id"):
        raise HTTPException(status_code=400, detail="Event must include trace_id.")
    EVENTS_INGESTED.inc()
    return ingest_live_event(event)


@app.post("/events/flush")
def flush_events():
    remaining = correlator.flush_all()
    _handle_closed_traces(remaining)
    final = assembler.flush()
    if final and _has_degraded_signal(final):
        _process_incident(final, "live-stream")
    _update_prediction_gauges()
    return {"status": "FLUSHED", "open_traces": correlator.open_trace_count}


@app.get("/dashboard/state")
def dashboard_state():
    return live_state.snapshot()


@app.post("/simulator/start")
async def simulator_start():
    simulator.start()
    return simulator.status()


@app.post("/simulator/stop")
async def simulator_stop():
    simulator.stop()
    return simulator.status()


@app.post("/simulator/inject")
def simulator_inject(request: SimulatorInjectRequest):
    try:
        return simulator.inject(request.fault)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/simulator/status")
def simulator_status():
    return simulator.status()


@app.get("/infra/status")
def infra_status():
    return infra.status()


@app.get("/kafka/stats")
def kafka_stats():
    result = infra.consumer_stats()
    return {
        "consumer_online": result["online"],
        "stats": result["stats"],
        "replay": dict(_replay_status),
    }


@app.post("/kafka/replay")
async def kafka_replay(request: KafkaReplayRequest):
    global _replay_task
    if _replay_task is not None and not _replay_task.done():
        raise HTTPException(status_code=409, detail="A replay is already in progress.")
    if not infra.probe_kafka():
        raise HTTPException(
            status_code=503,
            detail="Kafka broker unreachable. Start it with: docker compose up kafka "
                   "(note: the API must run on the host — the broker advertises localhost:9092).",
        )

    sample_logs_dir = os.path.join(WORKSPACE_ROOT, "backend", "sample_logs")
    log_path = os.path.join(sample_logs_dir, os.path.basename(request.scenario))
    if not os.path.exists(log_path):
        raise HTTPException(status_code=404, detail=f"Scenario '{request.scenario}' not found.")

    with open(log_path, "r", encoding="utf-8") as f:
        events = [e for e in (parse_log_line(line) for line in f) if e]
    if not events:
        raise HTTPException(status_code=400, detail="Scenario file contains no parseable events.")

    _replay_status.update(state="starting", scenario=request.scenario,
                          sent=0, total=len(events), error=None)
    _replay_task = asyncio.create_task(_run_kafka_replay(events, request.rate))
    return dict(_replay_status)


WAR_ROOM_FILES = (
    "incident_summary.md",
    "incident_timeline.md",
    "incident_graph.md",
    "postmortem.md",
    "telemetry_db.csv",
    "suggested_patch.diff",
)
WAR_ROOM_MAX_BYTES = 100_000


@app.get("/war-room")
def war_room_list():
    war_room_dir = os.path.join(WORKSPACE_ROOT, "active_war_room")
    files = []
    for name in WAR_ROOM_FILES:
        path = os.path.join(war_room_dir, name)
        if os.path.exists(path):
            files.append({"name": name, "available": True, "mtime": os.path.getmtime(path)})
        else:
            files.append({"name": name, "available": False, "mtime": None})
    return {"files": files}


@app.get("/war-room/{filename}")
def war_room_file(filename: str):
    if filename not in WAR_ROOM_FILES:
        raise HTTPException(status_code=404, detail="Unknown artifact.")
    path = os.path.join(WORKSPACE_ROOT, "active_war_room", filename)
    if not os.path.exists(path):
        return {"available": False, "name": filename, "content": None}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read(WAR_ROOM_MAX_BYTES)
    return {"available": True, "name": filename, "content": content,
            "mtime": os.path.getmtime(path)}


@app.get("/eval/scorecard")
def eval_scorecard():
    path = os.path.join(WORKSPACE_ROOT, "backend", "eval", "scorecard.md")
    if not os.path.exists(path):
        return {"available": False, "content": None}
    with open(path, "r", encoding="utf-8") as f:
        return {"available": True, "content": f.read(WAR_ROOM_MAX_BYTES)}


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

        batch_correlator = StreamingCorrelator(wall_idle=float("inf"), grace=120.0)
        for event in parsed_events:
            batch_correlator.ingest(event)
        emitted_traces = batch_correlator.flush_all()

        if not emitted_traces:
            raise HTTPException(status_code=400, detail="No valid trace IDs found in log events.")

        batch_assembler = IncidentAssembler()
        for trace in emitted_traces:
            batch_assembler.add(trace)
        incident_events = batch_assembler.flush()

        if not incident_events:
            raise HTTPException(status_code=400, detail="No incident assembled from traces.")

        result = run_incident_pipeline(incident_events, request.log_filename, WORKSPACE_ROOT)

        EVENTS_INGESTED.inc(len(parsed_events))
        TRACES_EMITTED.inc(len(emitted_traces))
        LATE_EVENTS.inc(batch_correlator.late_count)

        return {
            "status": "SUCCESS",
            "message": f"Successfully ingested {len(lines)} telemetry rows across {len(emitted_traces)} traces.",
            "correlation": {
                "traces_correlated": len(emitted_traces),
                "incident_events": len(incident_events),
                "late_events": batch_correlator.late_count,
            },
            "root_cause_analysis": {
                "ranked_root_causes": result.rca_result["ranked_root_causes"],
                "roles": result.rca_result["roles"],
                "cycle": result.rca_result["cycle"],
                "edge_basis": result.rca_result["edge_basis"],
            },
            "incident_report": {
                "incident_id": result.report.incident_id,
                "title": result.report.title,
                "overall_severity": result.report.overall_severity,
                "degraded_services": result.blast_radius["degraded_services"],
                "estimated_affected_requests": result.blast_radius["estimated_affected_requests"],
                "blast_radius_percentage": f"{result.blast_radius['blast_radius_percentage']:.1f}%",
                "hypotheses_count": len(result.report.hypotheses),
                "primary_mitigation_file": result.report.primary_remediation.file_path
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
_FRONTEND_DIST = os.path.join(WORKSPACE_ROOT, "frontend", "dist")
if os.path.isdir(_FRONTEND_DIST):
    app.mount("/", StaticFiles(directory=_FRONTEND_DIST, html=True), name="frontend")

