"""
The web API. It connects all the other pieces:

- POST /simulate/{fault_name} makes a fake outage and puts its events on
  Kafka, as real services would.
- A background task reads those events back from Kafka and feeds them to the
  correlator. When a trace closes, it runs root-cause analysis and the ML
  risk scoring.
- GET /incidents, /predictions, /ml/info and /metrics let you look at results.
"""

import asyncio
import random
from pathlib import Path

from fastapi import FastAPI
from prometheus_client import Counter, generate_latest
from starlette.responses import Response

import kafka_io
import ml
import rca
import simulator
from correlator import Correlator

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"

EVENTS_INGESTED = Counter("aegis_events_ingested_total", "Telemetry events consumed from Kafka")
INCIDENTS_PROCESSED = Counter("aegis_incidents_processed_total", "Incidents fully analyzed")

app = FastAPI(title="Aegis")

correlator = Correlator()
detector = ml.FailureDetector.load(ARTIFACTS_DIR / "model.joblib", ARTIFACTS_DIR / "feature_distribution.json")
# Results are kept in memory only (lost on restart), newest first.
incidents = []     # root-cause results
predictions = []   # ML risk scores
MAX_KEPT = 20      # each list keeps only this many

# Every simulated episode starts its clock near 0. But this server runs one
# Correlator for its whole life, and its watermark only moves forward. A
# second episode with small timestamps would look "long past" and be closed
# after a single event. So each new episode is shifted to start after the
# previous one ended.
_next_episode_start = 0.0


def handle_event(event):
    """Called for every event that arrives from Kafka."""
    EVENTS_INGESTED.inc()
    correlator.add_event(event)
    for sealed_trace in correlator.close_finished_traces():
        process_trace(sealed_trace)


def process_trace(events):
    """Analyzes one finished trace and stores the result."""
    rca_result = rca.analyze(events)
    if not rca_result["ranked_root_causes"]:
        return  # nothing failed in this trace

    if detector is not None:
        _score_shadow_predictions(events)

    incidents.insert(0, rca_result)
    del incidents[MAX_KEPT:]
    INCIDENTS_PROCESSED.inc()


def _score_shadow_predictions(events):
    # Shadow mode: record what the ML model thinks, but don't act on it. The
    # root cause always comes from rca.py.
    seen = []
    for event in events:
        # Skip services the model wasn't trained on ("api"). A score for them
        # would be meaningless, and would also confuse the drift monitor.
        if event["service"] not in ml.TRAINED_SERVICES:
            seen.append(event)
            continue

        features = ml.extract_features(seen, event)
        risk = float(detector.predict_risk(features))
        predictions.insert(0, {
            "service": event["service"],
            "severity": event["severity"],
            "risk": risk,
            "high_risk": risk >= ml.RISK_THRESHOLD,
        })
        del predictions[MAX_KEPT:]
        seen.append(event)


async def close_idle_traces_forever():
    # Every 2 seconds, close traces that have gone quiet. Without this, a
    # single outage with nothing after it would never close (see correlator.py).
    while True:
        await asyncio.sleep(2)
        for sealed_trace in correlator.close_wall_idle_traces():
            process_trace(sealed_trace)


@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(kafka_io.consume_forever(handle_event))
    asyncio.create_task(close_idle_traces_forever())


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/simulate/{fault_name}")
async def simulate(fault_name: str):
    global _next_episode_start
    if fault_name not in simulator.FAULT_TYPES:
        return {"error": f"unknown fault_name, choose one of {list(simulator.FAULT_TYPES)}"}
    # A new random seed each time gives a new trace_id. Reusing a trace_id
    # would make the correlator drop every event as too late, because it
    # already closed that trace.
    seed = random.randint(0, 1_000_000)
    events, root_cause_service, _, _ = simulator.generate_episode(seed=seed, fault_name=fault_name)

    for event in events:
        event["timestamp"] += _next_episode_start
    _next_episode_start = max(event["timestamp"] for event in events) + 60

    await kafka_io.send_events(events)
    return {"status": "sent", "event_count": len(events), "expected_root_cause": root_cause_service}


@app.get("/incidents")
def list_incidents():
    return incidents


@app.get("/predictions")
def list_predictions():
    return predictions


@app.get("/ml/info")
def ml_info():
    if detector is None:
        return {"available": False}
    return {"available": True, "risk_threshold": ml.RISK_THRESHOLD, "drift": detector.drift_monitor.status()}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type="text/plain")
