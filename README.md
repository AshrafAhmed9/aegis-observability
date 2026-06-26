# Aegis — AI-Native Incident Correlation & Observability Platform

> **Streaming telemetry correlation engine with event-time watermarks, topological root-cause analysis (Kahn's), and Groq LLaMA 3.3 70B AI augmentation — built for distributed systems incident debugging.**

[![Python](https://img.shields.io/badge/Python-3.8+-3776ab?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231f20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Prometheus](https://img.shields.io/badge/Prometheus-Instrumented-e6522c?style=flat&logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboard-f46800?style=flat&logo=grafana&logoColor=white)](https://grafana.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What Changed in v2

v1 was a batch prototype — it read a file, analyzed the first trace, and relied on canned LLM fallbacks to look correct. **The deterministic engine was actually analyzing a healthy trace on a critical incident.** v2 fixes the architecture:

| Capability | v1 | v2 |
|---|---|---|
| Correlation | Batch group-by, single trace (`traces[0]`) | **Streaming windowed correlation with event-time watermarks** |
| Root cause | None (LLM guessed) | **Kahn's topological sort on reversed call graph** |
| Ingestion | File read | **HTTP events + Kafka consumer group** |
| LLM output | Unvalidated `raw["key"]` | **Schema-validated with deterministic fallback** |
| Eval | None | **3/3 scorecard against engine ground truth** |
| Observability | None | **Prometheus metrics + Grafana** |
| Scaling | N/A | **Kafka partitioned by trace_id, KEDA scaling design** |

---

## Quick Start

### Option 1: API Only (fastest)

```bash
git clone https://github.com/AshrafAhmed9/aegis-observability.git
cd aegis-observability
git checkout v2-streaming
cd backend

copy .env.example .env   # Add your free Groq key (or skip — deterministic fallback works without it)
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8000
```

Test:
```bash
curl -X POST http://127.0.0.1:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"log_filename": "redis_retry_storm.log"}'
```

### Option 2: Full Stack (Kafka + Prometheus + Grafana)

```bash
git clone https://github.com/AshrafAhmed9/aegis-observability.git
cd aegis-observability
git checkout v2-streaming

# Start infrastructure
docker-compose up kafka prometheus grafana

# Terminal 2: Start consumer
cd backend
pip install -r requirements.txt
python -m app.consumer

# Terminal 3: Replay events through Kafka
cd backend
python producers/replay_producer.py --scenario redis_retry_storm.log --sink kafka --rate 5
```

- **API docs**: http://127.0.0.1:8000/docs
- **Prometheus metrics**: http://127.0.0.1:8000/metrics
- **Grafana**: http://localhost:3000 (admin / aegis)

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
  │  │  • Flattens traces → service graph    │   │
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
  │  │  • Deterministic scoring              │   │
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

The core architectural principle: **the deterministic engine is ground truth; the LLM only interprets.**

| Layer | Responsibility | Trust Level |
|---|---|---|
| Streaming Correlator | Event-time windowing, trace grouping, incident assembly | **Ground truth** |
| Topological RCA | Root-vs-symptom classification via Kahn's algorithm | **Ground truth** |
| AI Layer | Hypothesis synthesis, natural language, patch generation | **Interpretive only** |

The LLM receives the deterministic RCA result as context in its prompt. If it returns invalid JSON, the system falls back to a report built entirely from the engine's output — no hard LLM dependency.

**Eval scorecard (deterministic engine vs ground truth):**

| Metric | Score |
|---|---|
| Root cause top-1 match | 3/3 |
| Root cause class match | 3/3 |
| Schema validity | 3/3 |
| Degraded services match | 3/3 |

---

## The Three Technical Pillars

### 1. Streaming Windowed Correlation (Dataflow Model)

Events arrive one at a time, possibly out of order. The `StreamingCorrelator` buffers per `trace_id` and uses an **event-time watermark** with a configurable grace period to tolerate late arrivals.

A trace closes when:
- **Idle gap**: no new event for N seconds of event-time (watermark advanced past it)
- **Max window**: event-time span exceeds cap (prevents memory leaks)
- **LRU eviction**: memory cap exceeded (oldest trace force-closed)

Late events (arriving after their trace closed) are counted as a quality metric, not silently dropped.

The `IncidentAssembler` then groups closed traces into a tumbling event-time window, because **causality crosses trace boundaries** — in the Redis scenario, the incident spans 11 separate trace IDs with no span links between the failing services.

### 2. Topological Root-vs-Symptom (Kahn's Algorithm)

Call edges run `caller → callee`. Failure propagates the **opposite direction**: callee fails → caller times out. The algorithm:

1. **Reverse the call graph** (edges become `callee → caller`)
2. **Run Kahn's topological sort** (deterministic via sorted seeding)
3. **Classify**: a degraded node with no degraded dependency = `ROOT_CAUSE`; downstream = `SYMPTOM`
4. **Cycle/deadlock detection**: Kahn residual = `CYCLE_MEMBER`; `err_class` containing "deadlock" = forced root

When no span links exist between failing services (the Redis scenario), the system falls back to **earliest-ERROR-in-event-time** ordering with lowered confidence — honestly signaled as `edge_basis: "EVENT_TIME"`.

### 3. Kafka Pipeline (Partition-Local Correlation)

Events are keyed by `trace_id` — every event of a trace lands on the same partition, so each consumer in the group correlates locally with **zero cross-instance coordination**. Adding consumers just reassigns partitions.

The `StreamingCorrelator` was designed as a lock-free, single-threaded state machine specifically so it runs unchanged inside the Kafka consumer's poll loop — no async leaks, no thread bridges.

---

## Prometheus Metrics

| Metric | Type | Description |
|---|---|---|
| `aegis_events_ingested_total` | Counter | Total events ingested |
| `aegis_traces_emitted_total` | Counter | Traces closed and emitted |
| `aegis_incidents_processed_total` | Counter | Incidents fully processed |
| `aegis_late_events_total` | Counter | Events arriving after trace closed |
| `aegis_open_traces` | Gauge | Currently buffered traces |
| `aegis_correlation_duration_seconds` | Histogram | Full pipeline processing time |
| `aegis_root_cause_class_total` | Counter | Root causes by class (labels) |

---

## Production Failure Scenarios

| Scenario | Root Cause (Engine) | Classification | Edge Basis |
|---|---|---|---|
| Redis Pool Exhaustion + Retry Storm | `redis-cache` (resource_exhaustion) | ROOT → celery/gateway SYMPTOM | EVENT_TIME |
| PostgreSQL Row Lock Deadlock | `postgres-db` (deadlock) | CYCLE_MEMBER → order/gateway SYMPTOM | SPAN |
| Cache Stampede + DB Starvation | `postgres-db` (resource_exhaustion) | ROOT → product/gateway SYMPTOM | SPAN |

---

## War Room Artifacts

Every `/ingest` call produces 6 artifacts in `active_war_room/`:

| File | Format | Contents |
|---|---|---|
| `incident_summary.md` | Markdown | RCA hypotheses, severity, blast radius |
| `incident_timeline.md` | Markdown table | Chronological trace event timeline |
| `incident_graph.md` | Mermaid flowchart | Color-coded failure propagation graph |
| `postmortem.md` | Markdown | SRE postmortem with prevention checklist |
| `telemetry_db.csv` | CSV | SQL-queryable trace database |
| `suggested_patch.diff` | Unified diff | Ready-to-review code remediation |

---

## Performance (Locust Load Test)

Deterministic pipeline only (LLM disabled — measures the engine you'd defend in an interview):

| Metric | Value |
|---|---|
| Concurrent users | 20 |
| Duration | 60 seconds |
| Total requests | 3,642 |
| `/ingest` requests | 2,793 |
| Error rate | **0%** |
| Throughput | **47 req/s** sustained |
| `/ingest` p50 latency | **10 ms** |
| `/ingest` p95 latency | **28 ms** |
| `/ingest` p99 latency | **45 ms** |
| `/ingest` max latency | 171 ms |
| `GET /` p50 | 3 ms |
| `GET /scenarios` p50 | 3 ms |

Each `/ingest` request runs the full pipeline: parse 17 log lines → stream-correlate 11 traces → assemble incident → build propagation graph → topological RCA (Kahn's) → export 6 war room artifacts.

With the Groq LLM enabled, `/ingest` p50 rises to ~20s (dominated by the external API call). The deterministic engine completes in <50ms — the AI layer is interpretive garnish, not a latency dependency.

> Measured with Locust 2.x, 20 concurrent users, 60s run, on a local machine. See `backend/tests/locustfile.py`.

---

## Design Tradeoffs

### Why deterministic correlation before AI?
LLMs hallucinate causal relationships. By running deterministic correlation and topological RCA first, every AI output is anchored to verified graph data — the LLM interprets a pre-validated structure, not raw logs.

### Why event-time watermarks instead of wall-clock windowing?
Wall-clock windowing makes results depend on replay speed, network lag, and machine load. Event-time windowing produces identical correlation results whether processing a live stream or replaying a 2026 log file — deterministic and reproducible.

### Why Kahn's algorithm for root cause?
Root cause detection in a service dependency graph is a topological ordering problem. Kahn's algorithm is O(V+E), deterministic (given sorted seeding), handles cycles (Kahn residual = deadlock detection), and produces an explainable result — no ML black box.

### Why key Kafka messages by trace_id?
Keying by `trace_id` ensures every event of a trace lands on the same partition. This means each consumer holds all state needed to correlate that trace locally — no cross-consumer coordination, no distributed state, no shuffle. Adding consumers just reassigns partitions.

### Why NOT Kubernetes (yet)?
K8s + KEDA autoscaling on consumer lag is designed but not deployed. A load test showed zero sustained consumer lag under the current workload — single-consumer throughput suffices. Single-pod K8s with no scaling signal is infrastructure theater. The scaling design is documented in `backend/scaling-design.md`.

---

## Scaling Design

K8s + KEDA autoscaling is **gated on evidence**: deploy only when `kafka_consumergroup_lag` builds faster than one consumer drains under sustained load. If lag never builds, the honest answer is that single-consumer throughput suffices — and that judgment is itself defensible.

See `backend/scaling-design.md` for the full design: KEDA ScaledObject config, partition strategy, gate conditions, and the load test results that determined the current decision.

---

## Project Structure

```
aegis-observability/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI endpoints (/ingest, /scenarios, /metrics)
│   │   ├── parser.py            # Structured KV trace log parser
│   │   ├── correlation.py       # Propagation graph + topological RCA (Kahn's)
│   │   ├── streaming.py         # StreamingCorrelator + IncidentAssembler
│   │   ├── analyzer.py          # Groq AI RCA with schema validation + deterministic fallback
│   │   ├── consumer.py          # Kafka consumer group (aiokafka)
│   │   ├── metrics.py           # Prometheus metric definitions
│   │   └── jetro_service.py     # War room artifact exporter
│   ├── producers/
│   │   └── replay_producer.py   # HTTP + Kafka replay with jitter/late injection
│   ├── eval/
│   │   ├── ground_truth.json    # Expected root causes per scenario
│   │   ├── run_eval.py          # Eval harness (3/3 scorecard)
│   │   └── scorecard.md         # Latest eval results
│   ├── observability/
│   │   └── prometheus.yml       # Prometheus scrape config
│   ├── tests/
│   │   ├── test_streaming.py    # 16 tests: correlator, assembler, helpers
│   │   └── test_rca.py          # 5 tests: topological RCA, deadlock, determinism
│   ├── sample_logs/
│   │   ├── redis_retry_storm.log
│   │   ├── pg_deadlock.log
│   │   └── cache_stampede.log
│   ├── scaling-design.md        # K8s/KEDA scaling design (gated)
│   └── requirements.txt
├── docker-compose.yml           # Kafka (KRaft) + Prometheus + Grafana
├── Dockerfile
└── README.md
```

---

## Running Tests

```bash
cd backend
pip install pytest
python -m pytest tests/ -v          # 21 tests
python eval/run_eval.py             # 3/3 scorecard
```

---

## Interview Reference

**Q: Why not just pass logs to an LLM?**
Raw logs lack causal structure. The deterministic engine establishes ground truth first — trace correlation, service-level propagation graph, topological root-cause ordering. The LLM receives this structured result and only interprets it. If the LLM fails or returns invalid output, the system falls back to a report built entirely from deterministic analysis.

**Q: How does the streaming correlator handle out-of-order events?**
Event-time watermark with a configurable grace period. The watermark tracks `max_event_time_seen - grace`. Any trace whose latest event is behind the watermark by more than the idle gap is closed. Late events (arriving after closure) are counted but not silently dropped — they're a quality metric. This is the same model as Google Dataflow/Apache Beam.

**Q: How do you determine root cause vs symptom?**
Reverse the call graph (failure flows callee → caller, opposite to calls) and run Kahn's topological sort. A degraded node with no degraded dependency beneath it is the root cause; everything downstream is a symptom. Cycles (Kahn residual) or deadlock markers in `err_class` are detected as their own root-cause class.

**Q: Why partition Kafka by trace_id?**
Every event of a trace lands on the same partition, so each consumer holds all state needed for that trace's correlation — zero cross-consumer coordination. Scaling is just partition reassignment. No distributed state, no shuffle.

**Q: When would you add Kubernetes?**
Only when a load test shows `kafka_consumergroup_lag` building faster than one consumer drains. I'd use KEDA to autoscale consumers on that lag metric. Without evidence of real lag, K8s is single-pod theater — and saying that is itself a defensible engineering judgment.

**Q: What's the eval harness?**
`run_eval.py` runs all three scenarios through the full pipeline (correlator → assembler → graph → RCA) and scores the engine's top-1 root cause against curated ground truth. Current score: 3/3 root cause match, 3/3 class match, 3/3 schema validity. This proves the "no hard LLM dependency" claim — the engine is correct with the LLM turned off.
