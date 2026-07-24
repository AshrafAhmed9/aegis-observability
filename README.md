# Aegis — AI-Native Incident Correlation & Observability Platform

> **Streaming telemetry correlation engine with event-time watermarks, topological root-cause analysis, a self-training ML lifecycle, and LLM-augmented diagnostics for distributed systems.**

[![Python](https://img.shields.io/badge/Python-3.12-3776ab?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-18-61dafb?style=flat&logo=react&logoColor=white)](https://react.dev)
[![scikit--learn](https://img.shields.io/badge/scikit--learn-1.5-f7931e?style=flat&logo=scikitlearn&logoColor=white)](https://scikit-learn.org)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231f20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Prometheus](https://img.shields.io/badge/Prometheus-Instrumented-e6522c?style=flat&logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboard-f46800?style=flat&logo=grafana&logoColor=white)](https://grafana.com)
[![Tests](https://img.shields.io/badge/Tests-69%20passing-brightgreen?style=flat)]()
[![CI](https://github.com/AshrafAhmed9/aegis-observability/actions/workflows/ci.yml/badge.svg)](https://github.com/AshrafAhmed9/aegis-observability/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

I treat incident debugging as a **deterministic graph problem first, and an AI interpretation problem second**. Raw telemetry flows through a streaming correlation engine that buffers events by trace using event-time watermarks, builds a service dependency graph from span parentage, and classifies root cause vs. downstream symptom with a topological sort — all before either the ML layer or the LLM layer ever sees the data.

On top of that deterministic core sits a genuine self-training ML lifecycle: a simulator generates its own labeled fault data, a gradient-boosted classifier trains on it and runs in shadow mode alongside the deterministic engine, and a PSI-based drift monitor watches whether live traffic still looks like what the model was trained on. The deterministic engine is always the system of record — the ML layer only ever augments it, never overrides it.

When an incident happens, an LLM (Groq) writes up a six-artifact incident report — summary, timeline, dependency graph, postmortem, raw telemetry, and a suggested patch — using the deterministic root cause as ground truth so it's explaining a decision the graph already made correctly, not guessing on its own. If the LLM is unavailable, a deterministic report takes over automatically. Nothing about the incident pipeline depends on the LLM being up.

**Key capabilities:**
- **Streaming correlation with event-time watermarks** — Kafka-fed, per-trace buffering, two independent closing rules (idle-gap and max-trace-age)
- **Topological root-cause analysis** — Kahn's algorithm over a cause → effect dependency graph, correct on all 78 evaluated incidents
- **Self-training ML lifecycle** — a `HistGradientBoostingClassifier` trained on simulator-generated data, run in shadow mode, evaluated on unseen episodes with a measured early-warning lead time
- **PSI-based drift monitoring** — flags when live traffic no longer resembles the model's training distribution
- **LLM-generated, six-artifact war rooms** — Groq-authored incident reports with automatic, deterministic fallback
- **Fault-injection simulator** — a small synthetic service fleet with 3 fault types, used for both the live demo and the evaluation harness
- **69 tests, one command eval harness** — `run_eval.py` regenerates the 78-episode benchmark and measures both RCA correctness and prediction lead time fresh, every run

This version is a full rebuild of the project from the ground up: fewer files, every one of them small enough to read end to end, no dead layers left over from earlier iterations. Same idea, same stack, meaningfully tighter implementation.

---

## Quick Start

```bash
git clone https://github.com/AshrafAhmed9/aegis-observability.git
cd aegis-observability/backend

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # optional: add a Groq API key — falls back to a deterministic report without one
python train.py        # trains the failure-prediction model on simulator-generated data
uvicorn main:app --port 8010
```

Open **http://127.0.0.1:8010**. In a separate terminal, run the frontend:

```bash
cd frontend
npm install
npm run dev
```

The frontend proxies API calls to `:8010`. Click a fault button to inject a failure and watch the incident appear once the correlator has processed it.

### Full stack (Kafka + Prometheus + Grafana)

```bash
docker-compose up kafka prometheus grafana
```

| Service | URL |
|---|---|
| API | http://127.0.0.1:8010 |
| API docs | http://127.0.0.1:8010/docs |
| Prometheus | http://localhost:9091 |
| Grafana | http://localhost:3000 (admin / aegis) |

---

## Evaluation

```bash
cd backend
python train.py      # trains the model, prints its measured early-warning lead time
python run_eval.py   # grades the correlation engine against 78 generated incidents
```

`run_eval.py` generates 78 labeled fault episodes (26 each of 3 fault types, via `simulator.py`), runs each one through the real correlator + RCA pipeline, and checks whether the predicted root cause matches the service that actually broke. It also measures the trained model's median early-warning lead time on 18 held-out episodes it never trained on.

Both numbers are genuinely measured every time this runs — nothing is hardcoded.

---

## Architecture

```
Kafka (topic: telemetry.raw)
        │
        ▼
correlator.py — buffers events per trace_id, closes a trace once
        │        the event-time watermark has moved far enough past it
        ▼
rca.py — builds a cause → effect graph from span parentage, then
        │  Kahn's algorithm finds which service has nothing causing it
        ▼
ml.py — a gradient boosting model scores the same events in
        │  shadow mode, plus a PSI-based drift monitor
        ▼
warroom.py — writes 6 incident artifacts (summary, timeline, graph,
              postmortem, telemetry CSV, suggested patch), via Groq
              with a deterministic fallback
```

`main.py` wires all of this together as a FastAPI app: a `/simulate/{fault_name}` endpoint publishes a fault episode to Kafka, a background task consumes it and runs the pipeline above, and `/incidents` lists what's been processed.

---

## Tests

```bash
cd backend
pytest
```

69 tests across the four things this project actually does: stream correlation (`test_correlator.py`, `test_rca.py`), failure prediction (`test_ml_prediction.py`), drift detection (`test_drift.py`), and fault-injection simulation (`test_simulator.py`).

---

## Project layout

```
backend/
  correlator.py     event-time watermark buffering
  rca.py             dependency graph + Kahn's topological sort
  simulator.py       synthetic fault episodes (demo traffic + eval data)
  ml.py              feature extraction, GBM shadow detector, PSI drift
  train.py           trains the model on simulator-generated data
  warroom.py         6-artifact incident reports (Groq + fallback)
  kafka_io.py        Kafka producer/consumer
  main.py            FastAPI app
  run_eval.py        78-episode correctness + lead-time evaluation
  tests/             69 tests
  observability/     Prometheus + Grafana config
frontend/
  src/App.jsx        single-page console
```
