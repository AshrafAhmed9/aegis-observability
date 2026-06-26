# Aegis — AI-Native Incident Correlation & Observability Platform

> **Streaming telemetry correlation engine with event-time watermarks, topological root-cause analysis, and LLM-augmented diagnostics for distributed systems.**

[![Python](https://img.shields.io/badge/Python-3.8+-3776ab?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231f20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Prometheus](https://img.shields.io/badge/Prometheus-Instrumented-e6522c?style=flat&logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboard-f46800?style=flat&logo=grafana&logoColor=white)](https://grafana.com)
[![Tests](https://img.shields.io/badge/Tests-21%20passing-brightgreen?style=flat)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

Aegis treats incident debugging as a **deterministic graph problem first, and an AI interpretation problem second**. Raw telemetry flows through a streaming correlation engine that groups events by trace, assembles cross-trace incidents, builds a service dependency graph, and classifies root cause vs downstream symptoms using topological ordering — all before the LLM layer sees any data.

**Key capabilities:**
- **Streaming windowed correlation** with event-time watermarks and configurable grace periods for late/out-of-order events (Dataflow/Beam windowing model)
- **Topological root-cause analysis** via Kahn's algorithm on the reversed service call graph
- **Kafka-based ingestion pipeline** with partition-local correlation (keyed by `trace_id`)
- **Schema-validated LLM output** with deterministic fallback — no hard AI dependency
- **Prometheus self-instrumentation** with Grafana dashboards
- **21 unit tests + 3/3 eval scorecard** against curated ground truth

---

## Quick Start

### API Only

```bash
git clone https://github.com/AshrafAhmed9/aegis-observability.git
cd aegis-observability
git checkout v2-streaming
cd backend

copy .env.example .env   # Add your Groq API key (optional — deterministic fallback works without it)
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8000
```

```bash
curl -X POST http://127.0.0.1:8000/ingest \
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
| API docs | http://127.0.0.1:8000/docs |
| Prometheus metrics | http://127.0.0.1:8000/metrics |
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
  │  root cause class, consumer lag              │
  └─────────────────────────────────────────────┘
```

---

## Operational Trust Boundary

The core architectural principle: **the deterministic engine produces ground truth; the LLM provides interpretive augmentation only.**

| Layer | Responsibility | Trust Level |
|---|---|---|
| Streaming Correlator | Event-time windowing, trace grouping, incident assembly | **Ground truth** |
| Topological RCA | Root-vs-symptom classification via Kahn's algorithm | **Ground truth** |
| AI Augmentation | Hypothesis synthesis, natural language, patch generation | **Interpretive** |

The LLM receives the deterministic RCA result as structured context in its prompt. If it returns invalid JSON or fails entirely, the system produces a complete diagnostic report from the engine's output alone.

**Eval scorecard (deterministic engine vs curated ground truth):**

| Metric | Score |
|---|---|
| Root cause top-1 match | 3/3 |
| Root cause class match | 3/3 |
| Schema validity | 3/3 |
| Degraded services match | 3/3 |

---

## Technical Deep Dive

### Streaming Windowed Correlation

Events arrive one at a time, potentially out of order. The `StreamingCorrelator` buffers events per `trace_id` and uses an **event-time watermark** with a configurable grace period to determine when a trace is complete.

A trace closes when any condition is met:
- **Idle gap**: no new event for N seconds of event-time (watermark advanced past it)
- **Max window**: event-time span exceeds the configured cap
- **LRU eviction**: open trace count exceeds memory cap (oldest trace force-closed)

Late events (arriving after their trace window closed) are counted as a quality metric, not silently dropped.

The `IncidentAssembler` groups closed traces into a tumbling event-time window and flattens them into a single service-level event list. This is necessary because **causality crosses trace boundaries** — in the Redis scenario, the incident spans 11 separate trace IDs with no span links between the failing services.

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

---

## Failure Scenarios

| Scenario | Root Cause (Deterministic) | Classification | Edge Basis |
|---|---|---|---|
| Redis Pool Exhaustion + Celery Retry Storm | `redis-cache` — resource_exhaustion | ROOT_CAUSE → celery-worker, api-gateway SYMPTOM | EVENT_TIME |
| PostgreSQL Row Lock Deadlock | `postgres-db` — deadlock | CYCLE_MEMBER → order-service, api-gateway SYMPTOM | SPAN |
| Cache Stampede + DB Connection Exhaustion | `postgres-db` — resource_exhaustion | ROOT_CAUSE → product-service, api-gateway SYMPTOM | SPAN |

---

## War Room Artifacts

Every `/ingest` call generates 6 artifacts in `active_war_room/`:

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
│   │   ├── main.py              # FastAPI endpoints (/ingest, /scenarios, /metrics)
│   │   ├── parser.py            # Structured key-value trace log parser
│   │   ├── correlation.py       # Propagation graph builder + topological RCA
│   │   ├── streaming.py         # StreamingCorrelator + IncidentAssembler
│   │   ├── analyzer.py          # LLM integration with schema validation + deterministic fallback
│   │   ├── consumer.py          # Kafka consumer group (aiokafka)
│   │   ├── metrics.py           # Prometheus metric definitions
│   │   └── jetro_service.py     # War room artifact exporter
│   ├── producers/
│   │   └── replay_producer.py   # HTTP + Kafka replay with jitter/late injection
│   ├── eval/
│   │   ├── ground_truth.json    # Expected root causes per scenario
│   │   ├── run_eval.py          # Evaluation harness
│   │   └── scorecard.md         # Latest evaluation results
│   ├── observability/
│   │   └── prometheus.yml       # Prometheus scrape configuration
│   ├── tests/
│   │   ├── __init__.py
│   │   ├── test_streaming.py    # 16 tests: correlator, assembler, helpers
│   │   ├── test_rca.py          # 5 tests: topological RCA, deadlock, determinism
│   │   └── locustfile.py        # Locust load test configuration
│   ├── sample_logs/
│   │   ├── redis_retry_storm.log
│   │   ├── pg_deadlock.log
│   │   └── cache_stampede.log
│   ├── .env.example             # GROQ_API_KEY template
│   ├── .gitignore
│   ├── requirements.txt
│   ├── run.bat                  # Windows quick-start launcher
│   └── scaling-design.md        # K8s/KEDA scaling design (gated on evidence)
├── screenshots/
│   ├── gif.gif                  # Demo walkthrough
│   ├── jetro_graph.png          # Failure propagation graph
│   ├── jetro_warroom.png        # War room overview
│   ├── swagger_ingest.png       # /ingest endpoint
│   └── swagger_scenarios.png    # /scenarios endpoint
├── docker-compose.yml           # Kafka (KRaft) + Prometheus + Grafana
├── Dockerfile
├── .gitignore
└── README.md
```

---

## Testing

```bash
cd backend
pip install pytest locust
python -m pytest tests/ -v                    # 21 unit tests
python eval/run_eval.py                       # 3/3 evaluation scorecard
locust -f tests/locustfile.py --headless \
  -u 20 -r 5 -t 60s                          # Load test
```

---

## FAQ

**How does the system handle out-of-order events?**
The streaming correlator maintains an event-time watermark calculated as `max_event_time_seen - grace_period`. Traces close only when the watermark advances past their latest event by the idle gap threshold. Events arriving after closure are counted as late (a quality signal) rather than silently dropped. This follows the same windowing model as Google Dataflow and Apache Beam.

**How is root cause distinguished from downstream symptoms?**
The call graph is reversed (failure propagates callee → caller, opposite to call direction) and topologically sorted using Kahn's algorithm. A degraded node with no degraded dependency beneath it is classified as `ROOT_CAUSE`; everything downstream is `SYMPTOM`. Cycles detected via Kahn's residual set or deadlock markers in `err_class` are classified as `CYCLE_MEMBER`.

**What happens if the LLM returns invalid output?**
The LLM response is validated against the `AegisDiagnosticReport` Pydantic schema using `model_validate`. On validation failure, JSON parse error, or API timeout, the system generates a complete diagnostic report from the deterministic RCA result — including root cause, severity, blast radius, and remediation guidance. The LLM is never a single point of failure.

**How does Kafka partitioning enable horizontal scaling?**
Messages are keyed by `trace_id`, so all events of a trace are assigned to the same partition. Each consumer in the group holds complete correlation state for its assigned partitions — no cross-consumer coordination required. Scaling the consumer group is a partition reassignment, not a state migration.

**Why isn't Kubernetes deployed?**
K8s + KEDA autoscaling on consumer lag is fully designed (`backend/scaling-design.md`) but intentionally not deployed. Load testing confirmed zero sustained consumer lag — a single consumer processes events faster than they arrive. The scaling design documents the exact conditions, KEDA configuration, and partition strategy for when production load justifies it.
