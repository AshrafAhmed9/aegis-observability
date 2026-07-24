"""
The API. Wires everything else together:

- POST /simulate/{fault_name} generates a fault episode and publishes its
  events to Kafka, just like a real service fleet would.
- A background task consumes those events from Kafka, feeds them through
  the correlator, and once a trace closes, runs RCA + the ML shadow
  detector + the war room report writer.
- GET /incidents lists what's been processed so far, for the frontend.
"""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, generate_latest
from starlette.responses import Response

import kafka_io
import ml
import rca
import simulator
import warroom
from correlator import Correlator

load_dotenv()

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
# Locally, active_war_room/ sits next to backend/. In Docker, backend's
# contents become the container root, so WAR_ROOM_DIR is overridden via
# docker-compose to point at the mounted volume instead.
WAR_ROOM_DIR = Path(os.environ.get("WAR_ROOM_DIR", Path(__file__).resolve().parent.parent / "active_war_room"))

EVENTS_INGESTED = Counter("aegis_events_ingested_total", "Telemetry events consumed from Kafka")
INCIDENTS_PROCESSED = Counter("aegis_incidents_processed_total", "Incidents fully analyzed")

app = FastAPI(title="Aegis")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

correlator = Correlator()
detector = ml.FailureDetector.load(ARTIFACTS_DIR / "model.joblib", ARTIFACTS_DIR / "feature_distribution.json")
incidents = []  # most-recently-processed incident reports, newest first


def handle_event(event):
    EVENTS_INGESTED.inc()
    correlator.add_event(event)
    for sealed_trace in correlator.close_finished_traces():
        process_trace(sealed_trace)


def process_trace(events):
    rca_result = rca.analyze(events)
    if not rca_result["degraded_services"]:
        return  # nothing failed in this trace, nothing to report

    if detector is not None:
        _score_shadow_predictions(events, rca_result)

    report = warroom.build_report(rca_result)
    warroom.write_artifacts(report, rca_result, events, WAR_ROOM_DIR)
    incidents.insert(0, report)
    INCIDENTS_PROCESSED.inc()


def _score_shadow_predictions(events, rca_result):
    # Shadow mode: score the ML detector's opinion, don't act on it. The
    # deterministic RCA result above is what actually drives the report.
    seen = []
    for event in events:
        features = ml.extract_features(seen, event, seen)
        detector.predict_risk(features)
        seen.append(event)


@app.on_event("startup")
async def start_kafka_consumer():
    asyncio.create_task(kafka_io.consume_forever(handle_event))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/simulate/{fault_name}")
async def simulate(fault_name: str):
    if fault_name not in simulator.FAULT_TYPES:
        return {"error": f"unknown fault_name, choose one of {list(simulator.FAULT_TYPES)}"}
    events, root_cause_service, _, _ = simulator.generate_episode(seed=hash(fault_name) % 10_000, fault_name=fault_name)
    await kafka_io.send_events(events)
    return {"status": "sent", "event_count": len(events), "expected_root_cause": root_cause_service}


@app.get("/incidents")
def list_incidents():
    return incidents[:20]


@app.get("/ml/info")
def ml_info():
    if detector is None:
        return {"available": False}
    return {"available": True, "risk_threshold": ml.RISK_THRESHOLD, "drift": detector.drift_monitor.status()}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type="text/plain")
