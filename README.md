# Aegis — AI-Native Incident Correlation & Observability Platform

> **Streaming telemetry correlation engine with event-time watermarks, topological root-cause analysis, online failure prediction, and LLM-augmented diagnostics for distributed systems.**

[![Python](https://img.shields.io/badge/Python-3.8+-3776ab?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-18-61dafb?style=flat&logo=react&logoColor=white)](https://react.dev)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231f20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Prometheus](https://img.shields.io/badge/Prometheus-Instrumented-e6522c?style=flat&logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboard-f46800?style=flat&logo=grafana&logoColor=white)](https://grafana.com)
[![Tests](https://img.shields.io/badge/Tests-40%20passing-brightgreen?style=flat)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

Aegis treats incident debugging as a **deterministic graph problem first, and an AI interpretation problem second**. Raw telemetry flows through a streaming correlation engine that groups events by trace, assembles cross-trace incidents, builds a service dependency graph, and classifies root cause vs downstream symptoms using topological ordering — all before the LLM layer sees any data.

**v3 adds a predictive layer on top of that same deterministic engine:** a lightweight online failure predictor (EWMA anomaly detection + OLS trend projection — no ML training, no model file, no new dependencies) forecasts resource exhaustion and rising error rates *minutes before* they cascade into an incident, with lead time measured and enforced by the eval harness. A simulated microservice fleet with a chaos-injection console demonstrates the full loop live in a browser: inject a fault, watch the prediction fire with a countdown, watch the failure actually cascade, watch Aegis correlate and root-cause it — all without a terminal.

**Key capabilities:**
- **Online failure prediction** — EWMA z-score anomaly detection + ordinary-least-squares trend projection (confidence = R², not a black-box score); forecasts threshold breaches with a measured **237-second lead time** in the eval harness
- **Simulated fleet + chaos console** — inject realistic faults (connection leak, queue backlog, sudden deadlock) into a live simulated service mesh and watch predict → fail → RCA happen end to end
- **React live SRE console** — service health grid with sparklines, active predictions with countdowns, propagation graph, incident feed — polling a `/dashboard/state` endpoint, no websockets, no chart libraries
- **Streaming windowed correlation** with event-time watermarks and configurable grace periods for late/out-of-order events (Dataflow/Beam windowing model)
- **Topological root-cause analysis** via Kahn's algorithm on the reversed service call graph
- **Kafka-based ingestion pipeline** with partition-local correlation (keyed by `trace_id`)
- **Schema-validated LLM output** with deterministic fallback — no hard AI dependency
- **Prometheus self-instrumentation** with Grafana dashboards
- **40 unit tests + 3/3 RCA eval scorecard + measured prediction lead time** against curated ground truth

---

## Quick Start

### Live Demo (Predict → Fail → RCA)

```bash
git clone https://github.com/AshrafAhmed9/aegis-observability.git
cd aegis-observability
git checkout v2-streaming
cd backend

cp .env.example .env   # Add your Groq API key (optional — deterministic fallback works without it)
pip install -r requirements.txt
python -m uvicorn app.main:app --port 8010
```

Open **http://127.0.0.1:8010** — click **Start fleet**, then **Inject: Redis connection leak**. The Active Predictions panel warns with a live countdown minutes before the failure actually cascades; once it does, the incident lands in the feed and a full postmortem is generated in `active_war_room/`.

To develop against the frontend live instead of the pre-built bundle: `cd frontend && npm install && npm run dev` (proxies to the API on :8010).

### Batch Analysis (API Only)

```bash
cd backend
python -m uvicorn app.main:app --reload --port 8010
```

```bash
curl -X POST http://127.0.0.1:8010/ingest \
  -H "Content-Type: application/json" \
  -d '{"log_filename": "redis_retry_storm.log"}'
```

### Full Stack (Kafka + Prometheus + Grafana)

```bash
git clone https://github.com/AshrafAhmed9/aegis-observability.git
cd aegis-observability && git checkout v2-streaming

# Terminal 1: Infrastructure
docker-compose up kafka prometheus grafana

# Terminal 2: Kafka consumer
cd backend && pip install -r requirements.txt
python -m app.consumer

# Terminal 3: Replay producer
cd backend
python producers/replay_producer.py --scenario redis_retry_storm.log --sink kafka --rate 5
```

| Service | URL |
|---|---|
| Live SRE console | http://127.0.0.1:8010/ |
| API docs | http://127.0.0.1:8010/docs |
| Prometheus metrics | http://127.0.0.1:8010/metrics |
| Grafana | http://localhost:3000 (admin / aegis) |

---

## Architecture

```text
  Replay Producer / HTTP API
           │
           ▼
  ┌─────────────────────────────────────────────┐
  │          Kafka (KRaft, single broker)        │
  │     topic: telemetry.raw                     │
  │     key: trace_id (partition-local state)    │
  └─────────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────────────────────────────────┐
  │     Consumer Group: aegis-correlators        │
  │                                              │
  │  ┌───────────────────────────────────────┐   │
  │  │  Streaming Windowed Correlator        │   │
  │  │  • Event-time watermark + grace       │   │
  │  │  • Per-trace_id buffering             │   │
  │  │  • Idle gap / max window close        │   │
  │  │  • Late event detection (side-output) │   │
  │  │  • LRU eviction at memory cap         │   │
  │  └───────────────────────────────────────┘   │
  │           │                                  │
  │           ▼                                  │
  │  ┌───────────────────────────────────────┐   │
  │  │  Incident Assembler                   │   │
  │  │  • Tumbling event-time window         │   │
  │  │  • Cross-trace incident aggregation   │   │
  │  └───────────────────────────────────────┘   │
  │           │                                  │
  │           ▼                                  │
  │  ┌───────────────────────────────────────┐   │
  │  │  Propagation Graph Builder            │   │
  │  │  • Nodes: services (severity, errors) │   │
  │  │  • Edges: span parentage or temporal  │   │
  │  └───────────────────────────────────────┘   │
  │           │                                  │
  │           ▼                                  │
  │  ┌───────────────────────────────────────┐   │
  │  │  Topological RCA (Kahn's Algorithm)   │   │
  │  │  • Reverse call graph                 │   │
  │  │  • ROOT_CAUSE / SYMPTOM / CYCLE       │   │
  │  │  • Deadlock detection via err_class   │   │
  │  │  • Deterministic confidence scoring   │   │
  │  └───────────────────────────────────────┘   │
  │           │                                  │
  │           ▼  [Operational Trust Boundary]    │
  │  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐   │
  │  │  Groq LLaMA 3.3 70B AI Layer         │   │
  │  │  • Schema-validated (model_validate)  │   │
  │  │  • Deterministic RCA-based fallback   │   │
  │  │  • Ground truth fed into prompt       │   │
  │  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘   │
  │           │                                  │
  │           ▼                                  │
  │  ┌───────────────────────────────────────┐   │
  │  │  War Room Export (6 artifacts)        │   │
  │  │  + Prometheus Metrics                 │   │
  │  └───────────────────────────────────────┘   │
  └─────────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────────────────────────────────┐
  │  Prometheus → Grafana Dashboard              │
  │  Metrics: ingest rate, late events,          │
  │  open traces, correlation latency,           │
  │  root cause class, prediction ETAs,          │
  │  consumer lag                                │
  └─────────────────────────────────────────────┘
```

### Live Predictive Path (HTTP)

The Kafka path above is the horizontal-scale story; the **live demo path** runs the identical `StreamingCorrelator` and RCA pipeline inside the FastAPI process itself, fed by a simulated fleet instead of a replay file:

```text
  Simulated Fleet (asyncio loop)         Chaos Console
  • healthy traffic, 3 injectable faults  (start/stop/inject)
           │  in-process event callback         │
           ▼                                    │
  ┌─────────────────────────────────────────────┴──┐
  │  FastAPI process (app/main.py)                  │
  │                                                  │
  │  StreamingCorrelator ──► IncidentAssembler       │
  │        │                       │                │
  │        ▼                       ▼                │
  │  FailurePredictor         RCA Pipeline           │
  │  (EWMA + OLS, per         (same as batch path)   │
  │   service/metric)               │                │
  │        │                       ▼                │
  │        └──────────────► LiveState (in-memory)    │
  │                                │                 │
  └────────────────────────────────┼─────────────────┘
                                   ▼
                     GET /dashboard/state (2s poll)
                                   ▼
                    React Console (predictions,
                    service grid, graph, incidents)
```

This is an honest architectural split, not a shortcut: the Kafka consumer is a separate OS process, so its in-memory state can't be read by the API process without adding IPC or a shared store — out of scope for what a lean demo needs. The Kafka path stays fully functional and observable via Prometheus/Grafana (`aegis_predictions_active`, `aegis_predicted_breach_eta_seconds`); it simply isn't wired into the same browser dashboard.

---

## Predictive Failure Detection

The predictor (`app/predictor.py`) is **pure statistics, not a trained model** — deliberately, so every number it produces is derivable on a whiteboard:

- **`MetricWindow`** keeps a rolling deque of `(event_time, value)` per `(service, metric)` pair, plus an EWMA mean/variance for anomaly scoring.
- **`TREND_BREACH`** fires when an ordinary-least-squares fit over that window has positive slope, **R² ≥ 0.6** (confidence *is* the R², not a proxy for one), and the projected time-to-threshold falls within a 600-second horizon.
- **`ANOMALY`** fires when the current value's z-score against the EWMA baseline exceeds 3.
- **`ERROR_RATE`** fires when a service's error count is measurably rising across a trailing 30-second event-time window.
- All three are computed against an internal **event-time watermark**, not wall-clock time — the same design principle as the correlator's windowing, so predictions are identical regardless of replay/simulation speed.
- Every prediction is deduplicated per `(service, metric, kind)` and re-armed with hysteresis (not just a falling edge) to avoid flapping.

**Measured result:** the eval harness (`eval/run_eval.py::evaluate_prediction`) drives the `redis_connection_leak` fault deterministically (fake clock, no real sleeps) and confirms a **237-second lead time** between the first trend warning and the actual breach — well past the 60-second pass threshold.

**A deliberate limit, not an oversight:** the `deadlock_burst` fault has no ramp — it's instantaneous, the way real deadlocks are. Trend projection cannot and does not predict it; it's only caught reactively via `ANOMALY`/`ERROR_RATE` once it's already happening, and root-caused normally by the RCA engine afterward. Including a fault the predictor *can't* see coming is intentional — it demonstrates the technique's boundaries rather than overclaiming.

**Scope note — hours-scale prediction:** the same EWMA/OLS technique generalizes to much slower signals (disk fill-up, memory leaks measured in hours) by downsampling to coarser time buckets before fitting the trend. That's not implemented here — the goal was a predict-fail-RCA loop demoable live in a browser in a few minutes, not a multi-hour observation window nobody would watch. See [Design Decisions](#design-decisions--tradeoffs) for the full reasoning.

---

## Chaos Console & Simulated Fleet

`app/simulator.py` runs a small fictional e-commerce topology (`api-gateway → checkout-service → {redis-cache, postgres-db, payment-worker}`) on an asyncio loop, generating realistic healthy traffic with proper trace/span parentage so the RCA graph resolves normally even with no fault active.

Three injectable faults, each with a **ramp → failure → recovery** lifecycle so the dashboard shows the whole arc, including the prediction clearing on recovery:

| Fault | Target metric | Predictable? | RCA class |
|---|---|---|---|
| `redis_connection_leak` | `connection_pool_usage` climbs to 1.0 over ~4 min | Yes — the hero demo | `resource_exhaustion` |
| `queue_backlog` | `payment-worker` queue depth climbs to 500+ | Yes | `resource_exhaustion` |
| `deadlock_burst` | sudden lock contention, no ramp | No — by design | `deadlock` |

Cascading symptoms (e.g. `api-gateway` timing out because `redis-cache` is exhausted) are deliberately delayed a few seconds behind the root-cause signal (`cascade_delay_seconds` in `FaultInjector`), mirroring the real propagation delay a request timeout takes to surface upstream — without it, the RCA's event-time tie-break can misattribute the root cause to whichever service happens to log first.

The React console (`frontend/`) is a three-tab, one-stop demo surface — no chart or diagram libraries anywhere:

- **Live Ops** — `ChaosPanel` (fault controls), `PredictionsPanel` (hero: countdown + confidence + progress bar), `ServiceGrid` (hand-rolled SVG sparklines), `PropagationGraph` (RCA roles as SVG columns), `IncidentFeed`, and `DemoTimeline` — an auto-checking milestone tracker (fleet started → fault injected → prediction fired → breach → RCA'd → postmortem) that computes the observed prediction lead time live.
- **Pipeline Map** — the README architecture diagram as a living UI: two lanes of stage nodes (HTTP/live lane and Kafka lane) with per-stage live counters, pulse-animated edges when data flows, controls embedded on the nodes (fault injection on the Simulator node, scenario replay on the Kafka producer node), and dimmed offline stages showing the exact command to bring them up. The Kafka lane is read via the consumer's own Prometheus exposition on :9095 — the same interface Prometheus scrapes.
- **Evidence** — the provisioned Grafana dashboard embedded in-page (anonymous viewer + allow-embedding, auto-provisioned datasource and dashboard via `backend/observability/grafana/`), plus an artifact viewer for the six war-room files and the eval scorecard.

A persistent `StatusStrip` shows live health dots for API/Kafka/Consumer/Prometheus/Grafana with bring-up commands on hover. Everything polls plain HTTP (2-5s) — no websockets.

---

## Operational Trust Boundary

The core architectural principle: **the deterministic engine produces ground truth; the LLM provides interpretive augmentation only.**

| Layer | Responsibility | Trust Level |
|---|---|---|
| Streaming Correlator | Event-time windowing, trace grouping, incident assembly | **Ground truth** |
| Topological RCA | Root-vs-symptom classification via Kahn's algorithm | **Ground truth** |
| Failure Predictor | EWMA/OLS trend & anomaly detection | **Ground truth** (pure statistics, no model) |
| AI Augmentation | Hypothesis synthesis, natural language, patch generation | **Interpretive** |

The LLM receives the deterministic RCA result as structured context in its prompt. If it returns invalid JSON or fails entirely, the system produces a complete diagnostic report from the engine's output alone.

**Eval scorecard (deterministic engine vs curated ground truth):**

| Metric | Score |
|---|---|
| Root cause top-1 match | 3/3 |
| Root cause class match | 3/3 |
| Schema validity | 3/3 |
| Degraded services match | 3/3 |
| Prediction lead time (`redis_connection_leak`) | **237s** (≥60s threshold) |

---

## Technical Deep Dive

### Streaming Windowed Correlation

Events arrive one at a time, potentially out of order. The `StreamingCorrelator` buffers events per `trace_id` and uses an **event-time watermark** with a configurable grace period to determine when a trace is complete.

A trace closes when any condition is met:
- **Idle gap**: no new event for N seconds of event-time (watermark advanced past it)
- **Max window**: event-time span exceeds the configured cap
- **Wall-clock idle** (live path only): no new event for N seconds of *real* time, so a trace isn't held open forever once a live stream stops advancing its own watermark
- **LRU eviction**: open trace count exceeds memory cap (oldest trace force-closed)

Late events (arriving after their trace window closed) are counted as a quality metric, not silently dropped.

The `IncidentAssembler` groups closed traces into a tumbling event-time window and flattens them into a single service-level event list. This is necessary because **causality crosses trace boundaries** — in the Redis scenario, the incident spans 11 separate trace IDs with no span links between the failing services. The batch `/ingest` path uses a 120-second window (one log file, force-flushed at the end); the live streaming path uses a 20-second window, since a long window on a continuously-running stream would bury a real failure under minutes of unrelated healthy traffic before it ever flushed.

### Topological Root Cause Analysis

Call edges in the propagation graph run `caller → callee`. Failure propagates the **opposite direction**: a callee fails, then its caller times out. The algorithm:

1. **Reverse the call graph** — edges become `callee → caller`
2. **Run Kahn's topological sort** with deterministic seeding (sorted node names)
3. **Classify** — a degraded node (severity >= ERROR) with no degraded dependency is `ROOT_CAUSE`; nodes downstream of a root cause are `SYMPTOM`
4. **Detect cycles** — Kahn residual (nodes with remaining in-degree) indicates a circular dependency; `err_class` containing "deadlock" triggers deadlock-specific classification

When no span-based edges exist between failing services, the system falls back to **earliest-ERROR-in-event-time** ordering with reduced confidence, explicitly signaled via `edge_basis: "EVENT_TIME"`.

### Kafka Pipeline

Events are keyed by `trace_id`, ensuring every event of a trace lands on the same partition. Each consumer in the group correlates traces locally with **zero cross-instance coordination** — no distributed state, no shuffle. Horizontal scaling is partition reassignment.

The `StreamingCorrelator` is a lock-free, single-threaded state machine designed to run identically in the FastAPI sync endpoint, an asyncio task, or a Kafka consumer poll loop.

---

## Performance

Benchmarked with Locust (20 concurrent users, 60-second sustained load, deterministic pipeline):

| Metric | Value |
|---|---|
| Throughput | **47 req/s** sustained |
| `/ingest` p50 latency | **10 ms** |
| `/ingest` p95 latency | **28 ms** |
| `/ingest` p99 latency | **45 ms** |
| `/ingest` max latency | 171 ms |
| Error rate | **0%** |
| Total requests served | 3,642 |

Each `/ingest` request executes the full pipeline: parse 17 log lines, stream-correlate 11 traces, assemble incident, build propagation graph, run topological RCA, and export 6 war room artifacts.

With the Groq LLM enabled, `/ingest` p50 rises to ~20s, dominated entirely by the external API call. The deterministic engine completes in under 50ms.

> Load test configuration: `backend/tests/locustfile.py`

---

## Prometheus Metrics

| Metric | Type | Description |
|---|---|---|
| `aegis_events_ingested_total` | Counter | Total telemetry events ingested |
| `aegis_traces_emitted_total` | Counter | Traces closed and emitted |
| `aegis_incidents_processed_total` | Counter | Incidents fully processed through the pipeline |
| `aegis_late_events_total` | Counter | Events arriving after their trace window closed |
| `aegis_open_traces` | Gauge | Currently buffered trace count |
| `aegis_correlation_duration_seconds` | Histogram | End-to-end incident processing time |
| `aegis_root_cause_class_total` | Counter | Root causes by classification (labels) |
| `aegis_predictions_active` | Gauge | Currently active failure predictions |
| `aegis_predicted_breach_eta_seconds` | Gauge | Seconds until predicted threshold breach (labels: service, metric) |
| `aegis_predictions_emitted_total` | Counter | Predictions emitted by kind (labels) |

---

## Failure Scenarios

| Scenario | Root Cause (Deterministic) | Classification | Edge Basis |
|---|---|---|---|
| Redis Pool Exhaustion + Celery Retry Storm | `redis-cache` — resource_exhaustion | ROOT_CAUSE → celery-worker, api-gateway SYMPTOM | EVENT_TIME |
| PostgreSQL Row Lock Deadlock | `postgres-db` — deadlock | CYCLE_MEMBER → order-service, api-gateway SYMPTOM | SPAN |
| Cache Stampede + DB Connection Exhaustion | `postgres-db` — resource_exhaustion | ROOT_CAUSE → product-service, api-gateway SYMPTOM | SPAN |
| Simulated Redis Connection Leak (live) | `redis-cache` — resource_exhaustion | Predicted 237s ahead, then ROOT_CAUSE → api-gateway SYMPTOM | EVENT_TIME |

---

## War Room Artifacts

Every incident (batch `/ingest` or live-stream) generates 6 artifacts in `active_war_room/`:

| File | Format | Contents |
|---|---|---|
| `incident_summary.md` | Markdown | Root cause hypotheses, severity, blast radius |
| `incident_timeline.md` | Markdown table | Chronological trace event timeline |
| `incident_graph.md` | Mermaid flowchart | Color-coded failure propagation graph |
| `postmortem.md` | Markdown | SRE postmortem with prevention action items |
| `telemetry_db.csv` | CSV | Queryable trace database |
| `suggested_patch.diff` | Unified diff | Targeted code remediation |

---

## Design Decisions & Tradeoffs

### Why deterministic correlation before AI?
LLMs are effective at synthesis but unreliable for causal reasoning over telemetry. Running deterministic correlation and topological RCA first ensures every AI output is anchored to verified graph structure. The LLM interprets a pre-validated result — it does not determine causality.

### Why event-time watermarks instead of wall-clock windowing?
Wall-clock windowing introduces non-determinism: results depend on replay speed, network latency, and machine load. Event-time windowing produces identical correlation output whether processing a live stream or replaying historical data — critical for reproducible evaluations and debugging.

### Why Kahn's algorithm for root cause detection?
Root cause identification in a service dependency graph is a topological ordering problem. Kahn's algorithm is O(V+E), deterministic given sorted seeding, naturally detects cycles (the residual set maps directly to deadlock detection), and produces an explainable, auditable result.

### Why pure statistics instead of a trained model for prediction?
An EWMA/OLS fit needs no training data, no model file, and no ML dependency — and every number it outputs (a slope, an R², a z-score) is independently verifiable by hand. On a portfolio project with a handful of synthetic scenarios, a "trained" model would be theater: overfit to the demo data and no more explainable than the statistics it would replace. The simpler technique is also the more defensible one in an interview.

### Why a simulated fleet instead of more static log scenarios?
Static log replay proves the RCA engine works once; it doesn't let anyone *drive* the system. The simulator turns Aegis into a live, interactive artifact — inject a fault, watch the countdown, watch the RCA land — while still using the exact same ingestion and correlation code path as the replay producer and Kafka consumer. Building a scaled-down "toy" pipeline just for the demo would have meant maintaining two systems.

### Why not predict failures hours in advance?
The same trend-projection technique generalizes to slower signals by downsampling to coarser time buckets, but hours-scale detection can't be demonstrated live — nobody is going to watch a terminal for three hours to see a prediction resolve. The lean choice was fast, demoable ramps (minutes, not hours) that still produce a real, eval-measured lead time, and a documented note that the technique extends further rather than an untested and undemonstrable claim.

### Why partition Kafka messages by trace_id?
Keying by `trace_id` ensures all events of a trace land on the same partition. Each consumer holds complete state for its assigned traces — no cross-consumer coordination, no distributed locking, no state shuffle. Horizontal scaling reduces to partition reassignment.

### Why a lock-free single-threaded correlator?
The `StreamingCorrelator` must run in three contexts: a synchronous FastAPI endpoint, an asyncio task, and a Kafka consumer poll loop. A lock-free, single-threaded state machine satisfies all three without thread bridges or async leaks. The design constraint is intentional — it eliminates an entire category of concurrency bugs.

### Why gate Kubernetes on observed consumer lag?
K8s + KEDA autoscaling on `kafka_consumergroup_lag` is designed and documented (`backend/scaling-design.md`), but not deployed. Load testing showed zero sustained consumer lag under the current workload — a single consumer processes events faster than they arrive. Infrastructure is added when metrics justify it, not before. See `backend/scaling-design.md` for the full KEDA configuration, partition strategy, and gate conditions.

---

## Project Structure

```
aegis-observability/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py              # FastAPI endpoints, live-path wiring, SPA static mount
│   │   ├── parser.py            # Structured key-value trace log parser
│   │   ├── correlation.py       # Propagation graph builder + topological RCA
│   │   ├── streaming.py         # StreamingCorrelator + IncidentAssembler
│   │   ├── predictor.py         # EWMA/OLS failure predictor (trend, anomaly, error-rate)
│   │   ├── simulator.py         # Simulated fleet + chaos fault injection
│   │   ├── live_state.py        # In-memory dashboard state registry
│   │   ├── pipeline.py          # Shared incident pipeline (batch + live paths)
│   │   ├── analyzer.py          # LLM integration with schema validation + deterministic fallback
│   │   ├── consumer.py          # Kafka consumer group (aiokafka)
│   │   ├── metrics.py           # Prometheus metric definitions
│   │   └── exporter.py          # War room artifact exporter
│   ├── producers/
│   │   └── replay_producer.py   # HTTP + Kafka replay with jitter/late injection
│   ├── eval/
│   │   ├── ground_truth.json    # Expected root causes per scenario
│   │   ├── run_eval.py          # RCA + prediction lead-time evaluation harness
│   │   └── scorecard.md         # Latest evaluation results
│   ├── observability/
│   │   └── prometheus.yml       # Prometheus scrape configuration
│   ├── tests/
│   │   ├── __init__.py
│   │   ├── test_streaming.py    # 18 tests: correlator, assembler, helpers, wall-idle closure
│   │   ├── test_rca.py          # 5 tests: topological RCA, deadlock, determinism
│   │   ├── test_predictor.py    # 6 tests: EWMA/OLS trend, anomaly, recovery, rate-independence
│   │   ├── test_simulator.py    # 6 tests: span chains, fault ramps, cascade ordering, lifecycle
│   │   └── locustfile.py        # Locust load test configuration
│   ├── sample_logs/
│   │   ├── redis_retry_storm.log
│   │   ├── pg_deadlock.log
│   │   └── cache_stampede.log
│   ├── .env.example             # GROQ_API_KEY template
│   ├── .gitignore
│   ├── requirements.txt
│   └── scaling-design.md        # K8s/KEDA scaling design (gated on evidence)
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── ChaosPanel.jsx       # Fault injection controls
│   │   │   ├── IncidentFeed.jsx     # Recent incidents ring buffer
│   │   │   ├── PredictionsPanel.jsx # Hero: countdown, confidence, progress bar
│   │   │   ├── PropagationGraph.jsx # RCA roles as SVG columns
│   │   │   ├── ServiceGrid.jsx      # Per-service status + sparklines
│   │   │   ├── Sparkline.jsx        # Hand-rolled SVG sparkline (no chart lib)
│   │   │   └── StatBar.jsx          # Totals + stream watermark clock
│   │   ├── api.js               # Polling hook + simulator control calls
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   └── styles.css
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── screenshots/
│   ├── gif.gif                  # Demo walkthrough
│   ├── jetro_graph.png          # Failure propagation graph
│   ├── jetro_warroom.png        # War room overview
│   ├── swagger_ingest.png       # /ingest endpoint
│   └── swagger_scenarios.png    # /scenarios endpoint
├── docker-compose.yml           # Kafka (KRaft) + Prometheus + Grafana
├── Dockerfile                   # Multi-stage: frontend build + backend runtime
├── .gitignore
└── README.md
```

---

## Testing

```bash
cd backend
pip install pytest locust
python -m pytest tests/ -v                    # 40 unit tests
python eval/run_eval.py                       # 3/3 RCA scorecard + prediction lead time
locust -f tests/locustfile.py --headless \
  -u 20 -r 5 -t 60s                          # Load test

cd ../frontend
npm install && npm run build                  # Production frontend build
```

---

## FAQ

**How does the system handle out-of-order events?**
The streaming correlator maintains an event-time watermark calculated as `max_event_time_seen - grace_period`. Traces close only when the watermark advances past their latest event by the idle gap threshold. Events arriving after closure are counted as late (a quality signal) rather than silently dropped. This follows the same windowing model as Google Dataflow and Apache Beam.

**How is root cause distinguished from downstream symptoms?**
The call graph is reversed (failure propagates callee → caller, opposite to call direction) and topologically sorted using Kahn's algorithm. A degraded node with no degraded dependency beneath it is classified as `ROOT_CAUSE`; everything downstream is `SYMPTOM`. Cycles detected via Kahn's residual set or deadlock markers in `err_class` are classified as `CYCLE_MEMBER`.

**How does the predictor avoid false positives?**
Three gates: an R² ≥ 0.6 confidence floor on the trend fit (a noisy-but-flat metric won't pass), hysteresis re-arming (a prediction only clears once the metric drops meaningfully below threshold, not on the first small dip), and dedup to one active prediction per `(service, metric, kind)`. All thresholds live in a single `TRACKED_METRICS` table in `predictor.py` for easy tuning.

**How does the dashboard show the Kafka pipeline if the consumer is a separate process?**
It reads the consumer through the consumer's own Prometheus metrics endpoint (:9095) — the exact same interface Prometheus scrapes. No shared memory, no Redis, no push coupling: the dashboard's Pipeline Map polls `/kafka/stats`, which fetches and parses that exposition. If the consumer is down, the lane renders dimmed with the command to start it.

**Why does the in-browser Kafka replay require running the API on the host?**
The compose broker advertises `localhost:9092` (`KAFKA_ADVERTISED_LISTENERS`), so any *container* that connects gets told to reconnect to itself — only host processes can produce/consume. This matches how the stack is oriented anyway (Prometheus scrapes `host.docker.internal`, the consumer runs on the host). The `/kafka/replay` endpoint returns a clear 503 explaining this when the broker is unreachable.

**Could this predict failures hours in advance?**
The same EWMA/OLS technique generalizes to slower signals (disk fill-up, memory leaks) by downsampling to coarser time buckets before fitting the trend. It isn't implemented here because an hours-long ramp can't be demonstrated live — see [Design Decisions](#design-decisions--tradeoffs).

**What happens if the LLM returns invalid output?**
The LLM response is validated against the `AegisDiagnosticReport` Pydantic schema using `model_validate`. On validation failure, JSON parse error, or API timeout, the system generates a complete diagnostic report from the deterministic RCA result — including root cause, severity, blast radius, and remediation guidance. The LLM is never a single point of failure.

**How does Kafka partitioning enable horizontal scaling?**
Messages are keyed by `trace_id`, so all events of a trace are assigned to the same partition. Each consumer in the group holds complete correlation state for its assigned partitions — no cross-consumer coordination required. Scaling the consumer group is a partition reassignment, not a state migration.

**Why isn't Kubernetes deployed?**
K8s + KEDA autoscaling on consumer lag is fully designed (`backend/scaling-design.md`) but intentionally not deployed. Load testing confirmed zero sustained consumer lag — a single consumer processes events faster than they arrive. The scaling design documents the exact conditions, KEDA configuration, and partition strategy for when production load justifies it.
