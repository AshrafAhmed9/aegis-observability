# Aegis — AI-Native Incident Correlation & Observability Platform

> **Streaming telemetry correlation engine with event-time watermarks, topological root-cause analysis, a self-training ML lifecycle, and LLM-augmented diagnostics for distributed systems.**

[![Python](https://img.shields.io/badge/Python-3.8+-3776ab?style=flat&logo=python&logoColor=white)](https://python.org)
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

Aegis treats incident debugging as a **deterministic graph problem first, and an AI interpretation problem second**. Raw telemetry flows through a streaming correlation engine that groups events by trace, assembles cross-trace incidents, builds a service dependency graph, and classifies root cause vs downstream symptoms using topological ordering — all before either the LLM layer or the ML layer sees any data.

**v3 added a statistical predictive layer** (EWMA anomaly detection + OLS trend projection — no training, no model file) that forecasts incidents minutes before they cascade, demoed live via a simulated fleet with a chaos-injection console.

**v4 adds a genuine, self-training ML layer on top of that same deterministic engine, wrapped in a real lifecycle** — not three models bolted on as a checkbox, but the full loop: Aegis generates its own labeled training data (the seeded simulator + a fake-clock technique turns a 300-second fault lifecycle into milliseconds of generation time), trains challenger models from it, evaluates them against the statistical baseline on genuinely unseen data, runs the ML challenger in shadow mode with a live scoreboard, monitors its own feature drift, and **retrains itself from a button in the browser with a promotion gate** — a challenger is only promoted if it measurably beats the current champion; otherwise it's rejected with a logged reason. The deterministic engine remains the system of record throughout; the ML layer only ever augments it.

**Key capabilities:**
- **Self-training ML lifecycle** — simulator-generated labeled datasets, gradient-boosted failure prediction and RCA ranking, versioned model registry, one-click gated retraining, live champion/challenger scoreboard, PSI-based drift monitoring — the entire loop runs in under a minute, in a browser
- **Trained failure-prediction model** (`HistGradientBoostingClassifier`) running in shadow alongside the statistical baseline, with a measured, apples-to-apples lead-time comparison on unseen simulated episodes
- **Learned RCA ranker** — an identity-free graph-feature classifier that independently reproduces the deterministic Kahn baseline's decisions, validated by agreement rate rather than by beating an already-correct baseline
- **Incident similarity search** — every new incident is matched against a corpus of past incidents (TF-IDF by default, optional sentence-transformer embeddings) and its 3 nearest neighbors are attached automatically
- **Online statistical failure prediction** — EWMA z-score anomaly detection + OLS trend projection (confidence = R², not a black-box score); forecasts threshold breaches with a measured **237-second lead time**
- **Simulated fleet + chaos console** — inject realistic faults into a live simulated service mesh and watch predict → fail → RCA happen end to end, including both the statistical and ML detectors racing each other
- **React live SRE console** (3 tabs) — predictions with countdowns and ML/STAT source badges, a champion/challenger scoreboard, a Model Card with a live retrain button, a Pipeline Map showing the whole architecture as an animated diagram, and an embedded Grafana + artifact viewer
- **Streaming windowed correlation** with event-time watermarks, and **topological root-cause analysis** via Kahn's algorithm — unchanged foundations from v2/v3
- **Kafka-based ingestion pipeline** with partition-local correlation, **schema-validated LLM output** with deterministic fallback
- **69 unit tests + 3/3 RCA eval scorecard + measured STAT-vs-ML prediction comparison** against curated ground truth and held-out simulated episodes

---

## Quick Start

### Live Demo (Predict → Fail → RCA, statistical layer only)

```bash
git clone https://github.com/AshrafAhmed9/aegis-observability.git
cd aegis-observability
cd backend

cp .env.example .env   # Add your Groq API key (optional — deterministic fallback works without it)
pip install -r requirements.txt
python -m uvicorn app.main:app --port 8010
```

Open **http://127.0.0.1:8010** — click **Start fleet**, then **Inject: Redis connection leak**. The Active Predictions panel warns with a live countdown minutes before the failure actually cascades; once it does, the incident lands in the feed and a full postmortem is generated in `active_war_room/`.

To develop against the frontend live instead of the pre-built bundle: `cd frontend && npm install && npm run dev` (proxies to the API on :8010).

### Enabling the ML Layer

Trained model artifacts are committed to the repo (`backend/ml/artifacts/`), so the ML layer works out of the box once its dependencies are installed — no training required to demo it:

```bash
cd backend
pip install -r requirements-ml.txt
python -m uvicorn app.main:app --port 8010
```

Now `/ml/info` reports `ml_available: true`, the Predictions panel shows both **STAT** and **ML** badged rows, the Live Ops Scoreboard tracks both detectors' hits, and the Evidence tab's **Model Card** shows real trained metrics with a working **Retrain** button.

To regenerate everything from scratch (fast — the whole 300s-per-episode dataset generates in milliseconds thanks to the fake-clock technique):

```bash
python ml/generate_dataset.py          # ~100 simulated episodes -> labeled CSVs
python ml/train_failure_model.py       # trains + versions the failure model
python ml/train_rca_ranker.py          # trains + versions the RCA ranker
python ml/build_incident_corpus.py     # builds the similarity-search corpus
```

Optional: `pip install sentence-transformers` for embedding-based similarity search (falls back to TF-IDF automatically otherwise — pulls in torch, ~1-2GB, so it's opt-in).

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
cd aegis-observability

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
  │  │  • + optional ML ranking, displayed   │   │
  │  │    alongside (never overriding it)    │   │
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
  │  │  + Similar-incidents attachment       │   │
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

The Kafka path above is the horizontal-scale story; the **live demo path** runs the identical `StreamingCorrelator` and RCA pipeline inside the FastAPI process itself, fed by a simulated fleet instead of a replay file — now with the ML layer running in shadow alongside the statistical predictor:

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
  │  FailurePredictor (STAT)  RCA Pipeline           │
  │  MLFailureDetector (ML)   (+ ML ranker,          │
  │  DriftMonitor              + similar incidents)  │
  │        │                       │                │
  │        ▼                       ▼                │
  │  Scoreboard              LiveState (in-memory)   │
  │  (STAT vs ML outcomes)         │                 │
  │        └───────────────────────┘                 │
  └────────────────────────────────┼─────────────────┘
                                   ▼
                     GET /dashboard/state (2s poll)
                                   ▼
                    React Console (predictions,
                    scoreboard, graph, incidents,
                    model card, retrain button)
```

This is an honest architectural split, not a shortcut: the Kafka consumer is a separate OS process, so its in-memory state can't be read by the API process without adding IPC or a shared store — out of scope for what a lean demo needs. The Kafka path stays fully functional and observable via Prometheus/Grafana; it runs its own `MLFailureDetector` instance too, it just isn't wired into the same browser dashboard or scoreboard.

---

## The ML Lifecycle

This is the actual differentiator of v4: not three sklearn models, but the full loop running inside the platform, demoable end to end from one button.

### 1. Data generation (`ml/generate_dataset.py`)

Every dataset comes from the seeded simulator (`app/simulator.py`), driven with a **fake clock** (the same technique the eval harness already used in v3): a full 300-second fault lifecycle — ramp, failure, recovery — generates in milliseconds because `datetime.now()` is monkeypatched and advanced manually alongside the fault's internal timer. One retrain cycle generates ~100 episodes (healthy + 3 fault types × 25 seeds) in well under a second.

**Leakage-safe labeling** for the failure-prediction dataset: a rolling 60-second window is labeled positive only if it belongs to the fault's actual target service *and* the failure onset is within a 120-second horizon after the window ends. Windows that fall inside the failure/recovery phase itself are dropped entirely (neither class) — a model must never see "the incident is already happening" as training signal for "predict it's about to happen." `deadlock_burst` (no ramp, no precursor signal) is deliberately excluded from positive labels — it stays in evaluation only, as an honest control showing what a trend-based model *can't* see coming.

**Identity-free features** (`ml/features.py`) — no service names, no one-hot encodings — are computed by the exact same function at training time and at inference time (`app/ml_predictor.py`, `app/rca_ranker.py` both import from `ml/features.py`), eliminating train/serve skew and forcing both models to learn structural/telemetry patterns rather than memorize "postgres-db → deadlock."

### 2. Training + honest results

**Failure-prediction model** (`ml/train_failure_model.py`): a `HistGradientBoostingClassifier`, split **by episode** (never by row, to avoid leaking one incident's windows across train/val), evaluated against a threshold chosen for ≥90% precision. An `IsolationForest` trained unsupervised on the same features is included as a comparison row.

| Model | PR-AUC | Precision | Recall | Median lead (label-capped) |
|---|---|---|---|---|
| GBM (supervised) | 0.9997 | 91% | 100% | 58s |
| IsolationForest (unsupervised) | 0.92 | 90% | 65% | 36s |

**On the near-perfect GBM numbers:** this is expected, not overfitting hidden by a bad split — the simulator's ramps are smooth, near-deterministic curves with small bounded noise (the same property that makes the OLS baseline work well), so a model combining slope and current value can separate the classes almost exactly. The honest takeaway is about *methodology* (leak-safe labels, episode-level splits, a real held-out unsupervised comparison), not about the specific number, which is inflated relative to what real, noisier production telemetry would show.

**Calibration — is the model's confidence trustworthy, not just its ranking?** PR-AUC and precision/recall only measure whether the model *ranks* positives above negatives; a model can separate classes perfectly while still being wildly overconfident (saying "99% risk" when it's actually right 60% of the time). `ml/train_failure_model.py` computes a full reliability curve on held-out data — predicted probability bucketed into 10 bins, compared against the actual observed positive rate in each bucket — plus two standard summary numbers:

| Metric | Value | Interpretation |
|---|---|---|
| Expected Calibration Error (ECE) | **0.0003** | weighted mean gap between predicted confidence and actual outcome rate across bins; 0 = perfect |
| Brier score | **0.0003** | mean squared error between predicted probability and actual outcome; rewards calibration *and* sharpness |

Both are near zero — on this dataset, when the model says "99.97% risk," it is in fact right essentially all the time. The full bin-by-bin table (mean predicted vs. actual rate, with sample counts) is stored in `metrics.json` per version and viewable in the Model Card under each version's collapsible calibration row. This is standard practice before trusting any classifier's probability output for a downstream decision (like a severity threshold or an alert), and it's a question ("is your model's confidence trustworthy?") most portfolio ML projects never actually answer.

**The real STAT-vs-ML comparison** — training-time metrics above are capped at the 120-second label horizon by construction, so comparing them directly to the v3 baseline's 237s would be misleading. `eval/run_eval.py::evaluate_stat_vs_ml` instead runs the **actual runtime classes** (`FailurePredictor` and `MLFailureDetector`, not reconstructed offline metrics) side by side over identical, genuinely unseen-seed episodes:

| Detector | Episodes caught | Median lead time |
|---|---|---|
| STAT (OLS trend) | 3/3 | **228s** |
| ML (GBM) | 3/3 | **117s** |

This isn't the ML model underperforming — it's a direct, explainable consequence of the 120-second label horizon: the model was never shown "this fails in 200+ seconds" examples, so it structurally can't fire earlier than its training horizon allows. STAT's OLS extrapolation has no such cap. This asymmetry — and being able to explain *why* it exists — is worth more in an interview than either number in isolation.

**RCA ranker** (`ml/train_rca_ranker.py`): a pointwise classifier over `candidate_features` (severity, error-order rank, graph degree/depth, error-class flags — no service identity), split by incident. On this synthetic dataset, the deterministic Kahn baseline is already **100% correct** on all 78 evaluated incidents — a direct result of the cascade-timing fix described below — so there's no headroom for the ranker to "beat" it. Its value is demonstrated by **agreement rate (100%)**: an independently-learned model, using only structural graph features and no hand-coded topological-sort logic, reaches the same answer as the deterministic engine. The frontend shows both rankings side by side and flags agreement/disagreement explicitly rather than picking a winner.

**Incident similarity** (`ml/build_incident_corpus.py`, `app/incident_memory.py`): ~48 incidents (45 simulated + the 3 curated scenarios) are embedded and stored. The encoder is auto-selected — `sentence-transformers/all-MiniLM-L6-v2` if installed, TF-IDF (scikit-learn) otherwise — and the corpus itself stores only plain text signatures (root-cause class, degraded services, error classes, representative messages — never raw timestamps), so switching encoders never requires rebuilding it. Verified live: querying with a real `redis_connection_leak` incident correctly surfaces other `resource_exhaustion` incidents ahead of unrelated `deadlock` ones.

### 3. Shadow deployment, scoreboard, drift, and gated retraining

- **Shadow mode**: `MLFailureDetector` runs alongside `FailurePredictor` in both the HTTP and Kafka paths (`app/main.py`, `app/consumer.py`), sharing the same `Prediction` interface — the frontend can't tell them apart except by a `kind` tag, which is exactly the point.
- **Live scoreboard** (`app/scoreboard.py`): pure bookkeeping on predictions and outcomes already flowing through the pipeline — hits, false alarms, first-to-fire wins, and mean observed lead time per source, with zero new external dependencies.
- **Drift monitor** (`app/drift.py`): a coarse 4-bucket Population Stability Index computed against the training-time feature-distribution snapshot (saved automatically at train time). PSI < 0.1 is stable, > 0.25 is an alert — standard industry thresholds, computed with nothing but `math.log`.
- **Versioned registry + gated retraining** (`app/model_registry.py`, `ml/retrain_pipeline.py`): clicking **Retrain** in the Model Card fires `POST /ml/retrain`, which runs a subprocess (so sklearn training never blocks the API's event loop) that regenerates the dataset with fresh random seeds, trains new challenger versions of both models, and applies a promotion gate — a challenger must not regress PR-AUC or lose more than 10% of median lead time (failure model) / not regress top-1 accuracy (ranker) to be promoted; otherwise it's saved as a rejected version with the reason recorded. **Verified live**: a real retrain cycle produced v2 for both models, both passed the gate, and the running server hot-reloaded the new champions without a restart — confirmed via `/ml/info` before and after.

---

## Chaos Console & Simulated Fleet

`app/simulator.py` runs a small fictional e-commerce topology (`api-gateway → checkout-service → {redis-cache, postgres-db, payment-worker}`) on an asyncio loop, generating realistic healthy traffic with proper trace/span parentage so the RCA graph resolves normally even with no fault active.

Three injectable faults, each with a **ramp → failure → recovery** lifecycle so the dashboard shows the whole arc, including predictions clearing on recovery:

| Fault | Target metric | Predictable? | RCA class |
|---|---|---|---|
| `redis_connection_leak` | `connection_pool_usage` climbs to 1.0 over ~4 min | Yes — the hero demo | `resource_exhaustion` |
| `queue_backlog` | `payment-worker` queue depth climbs to 500+ | Yes | `resource_exhaustion` |
| `deadlock_burst` | sudden lock contention, no ramp | No — by design | `deadlock` |

Cascading symptoms (e.g. `api-gateway` timing out because `redis-cache` is exhausted) are deliberately delayed a few seconds behind the root-cause signal (`cascade_delay_seconds` in `FaultInjector`), mirroring the real propagation delay a request timeout takes to surface upstream — without it, the RCA's event-time tie-break can misattribute the root cause to whichever service happens to log first.

The React console (`frontend/`) is a three-tab, one-stop demo surface — no chart or diagram libraries anywhere:

- **Live Ops** — `ChaosPanel` (fault controls), `PredictionsPanel` (hero: countdown + confidence + progress bar + **ML/STAT source badge**), `ScoreboardStrip` (live champion/challenger tallies), `ServiceGrid` (hand-rolled SVG sparklines), `PropagationGraph` (RCA roles as SVG columns), `IncidentFeed` (expandable rows showing the ML ranking, agreement badge, and similar past incidents), and `DemoTimeline` — an auto-checking milestone tracker (fleet started → fault injected → prediction fired → **ML risk flagged** → breach → RCA'd → postmortem) that computes the observed prediction lead time live.
- **Pipeline Map** — the architecture diagram as a living UI: two lanes of stage nodes (HTTP/live lane and Kafka lane) with per-stage live counters including a dedicated **ML Layer** node (champion versions, active ML risk count, drift status), pulse-animated edges when data flows, controls embedded on the nodes, and dimmed offline stages showing the exact command to bring them up.
- **Evidence** — the provisioned Grafana dashboard embedded in-page, the **Model Card** (version history for both models, drift status, gate verdicts, and the live **Retrain** button), and an artifact viewer for the six war-room files plus the eval scorecard.

A persistent `StatusStrip` shows live health dots for API/Kafka/Consumer/Prometheus/Grafana with bring-up commands on hover. Everything polls plain HTTP (2-5s) — no websockets.

---

## Operational Trust Boundary

The core architectural principle: **the deterministic engine produces ground truth; the LLM and ML layers provide interpretive augmentation only.**

| Layer | Responsibility | Trust Level |
|---|---|---|
| Streaming Correlator | Event-time windowing, trace grouping, incident assembly | **Ground truth** |
| Topological RCA | Root-vs-symptom classification via Kahn's algorithm | **Ground truth** |
| Statistical Predictor | EWMA/OLS trend & anomaly detection | **Ground truth** (pure statistics, no model) |
| Trained ML Models | Failure risk scoring, RCA ranking, similarity search | **Shadow / advisory** — shown alongside, never overriding |
| AI Augmentation | Hypothesis synthesis, natural language, patch generation | **Interpretive** |

The LLM receives the deterministic RCA result as structured context in its prompt. If it returns invalid JSON or fails entirely, the system produces a complete diagnostic report from the engine's output alone. The ML layer is entirely additive — every endpoint and runtime module degrades silently to "not available" with zero ML dependencies installed.

**Eval scorecard:**

| Metric | Score |
|---|---|
| Root cause top-1 match | 3/3 |
| Root cause class match | 3/3 |
| Schema validity | 3/3 |
| Degraded services match | 3/3 |
| STAT prediction lead time (`redis_connection_leak`) | **237s** (≥60s threshold) |
| STAT vs ML lead time, unseen episodes | **228s vs 117s** (both 3/3 catch rate) |
| RCA ranker vs Kahn agreement | **100%** (15 held-out incidents) |

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
| `aegis_predictions_active` | Gauge | Currently active failure predictions (STAT + ML combined) |
| `aegis_predicted_breach_eta_seconds` | Gauge | Seconds until predicted threshold breach (labels: service, metric) |
| `aegis_predictions_emitted_total` | Counter | Predictions emitted by kind (labels) |

---

## Failure Scenarios

| Scenario | Root Cause (Deterministic) | Classification | Edge Basis |
|---|---|---|---|
| Redis Pool Exhaustion + Celery Retry Storm | `redis-cache` — resource_exhaustion | ROOT_CAUSE → celery-worker, api-gateway SYMPTOM | EVENT_TIME |
| PostgreSQL Row Lock Deadlock | `postgres-db` — deadlock | CYCLE_MEMBER → order-service, api-gateway SYMPTOM | SPAN |
| Cache Stampede + DB Connection Exhaustion | `postgres-db` — resource_exhaustion | ROOT_CAUSE → product-service, api-gateway SYMPTOM | SPAN |
| Simulated Redis Connection Leak (live) | `redis-cache` — resource_exhaustion | STAT predicts 228s ahead / ML 117s ahead, then ROOT_CAUSE → api-gateway SYMPTOM | EVENT_TIME |

---

## War Room Artifacts

Every incident (batch `/ingest` or live-stream) generates 6 artifacts in `active_war_room/`, plus 3 similar past incidents attached to the in-memory incident record:

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

### Why deterministic correlation before AI or ML?
LLMs are effective at synthesis but unreliable for causal reasoning over telemetry; trained models are only as good as the (necessarily limited) data they're trained on. Running deterministic correlation and topological RCA first ensures every AI or ML output is anchored to verified graph structure — both layers interpret or augment a pre-validated result, neither determines causality.

### Why pure statistics *and* a trained model for prediction, not just one?
The statistical predictor (EWMA/OLS) needs no training data and every number it outputs is verifiable by hand — it's the honest baseline. The trained GBM demonstrates a different, complementary skill: feature engineering, leak-safe labeling, and a measurable train/eval methodology. Running both in shadow and comparing them honestly (including where the ML model is *worse*, and why) is a stronger interview story than picking one and hiding the other's tradeoffs.

### Why generate training data from the simulator instead of collecting more real logs?
The simulator already exists (built for the live demo) and is seeded/deterministic, so it doubles as a free, exact-label, leakage-controllable data generator — every fault's target service and onset time are known exactly, at generation time, with zero manual labeling. The honest cost is that synthetic ramps are cleaner than real production noise, which is why the near-perfect PR-AUC numbers are explicitly caveated rather than presented as a headline claim.

### Why regenerate the dataset on every retrain instead of keeping one frozen validation set?
Each retrain uses fresh random seeds so the demo shows genuine variation run to run, not the same frozen answer. The honest tradeoff: the champion's original metrics and a new challenger's metrics come from different i.i.d. draws of the same underlying simulator distribution rather than one canonical held-out set — an appropriate simplification for a demo-grade lifecycle, not full rigorous cross-validation, and documented as such.

### Why does the RCA ranker only claim "agreement" instead of "beats the baseline"?
Because on this dataset, it's true: the deterministic Kahn baseline is already 100% correct (a direct consequence of the cascade-timing fix — see below), leaving no headroom to improve on. Claiming outperformance where there is none would be dishonest; claiming agreement — an independently-learned model reaching the same answer using only structural graph features — is a real, checkable, and still resume-worthy result.

### Why identity-free features for both ML models?
Without this constraint, a model can trivially memorize "postgres-db → deadlock" from a handful of fault archetypes instead of learning the actual structural signal (severity, timing order, graph position, slope). Excluding service names and one-hot encodings forces both models to learn something that would generalize to a service they've never seen — the harder, more honest problem.

### Why gate retraining on a promotion check instead of always promoting the newest version?
A newest-wins policy would make "retrain" theater — click a button, watch a number change, no real decision made. The gate (PR-AUC/accuracy must not regress, lead time may not regress by more than 10%) makes the retrain loop an actual MLOps decision with a real, loggable, sometimes-negative outcome — the demo is honest about rejections, not just promotions.

### Why event-time watermarks instead of wall-clock windowing?
Wall-clock windowing introduces non-determinism: results depend on replay speed, network latency, and machine load. Event-time windowing produces identical correlation output whether processing a live stream or replaying historical data — critical for reproducible evaluations and debugging.

### Why Kahn's algorithm for root cause detection?
Root cause identification in a service dependency graph is a topological ordering problem. Kahn's algorithm is O(V+E), deterministic given sorted seeding, naturally detects cycles (the residual set maps directly to deadlock detection), and produces an explainable, auditable result.

### Why a simulated fleet instead of more static log scenarios?
Static log replay proves the RCA engine works once; it doesn't let anyone *drive* the system, and it can't generate the labeled training data the ML layer needs. The simulator turns Aegis into a live, interactive artifact and a data factory simultaneously, using the exact same ingestion and correlation code path as the replay producer and Kafka consumer.

### Why not predict failures hours in advance?
The same trend-projection technique generalizes to slower signals by downsampling to coarser time buckets, but hours-scale detection can't be demonstrated live — nobody is going to watch a terminal for three hours to see a prediction resolve. The lean choice was fast, demoable ramps (minutes, not hours) that still produce a real, eval-measured lead time.

### Why partition Kafka messages by trace_id?
Keying by `trace_id` ensures all events of a trace land on the same partition. Each consumer holds complete state for its assigned traces — no cross-consumer coordination, no distributed locking, no state shuffle. Horizontal scaling reduces to partition reassignment.

### Why a lock-free single-threaded correlator?
The `StreamingCorrelator` must run in three contexts: a synchronous FastAPI endpoint, an asyncio task, and a Kafka consumer poll loop. A lock-free, single-threaded state machine satisfies all three without thread bridges or async leaks. The design constraint is intentional — it eliminates an entire category of concurrency bugs.

### Why gate Kubernetes on observed consumer lag?
K8s + KEDA autoscaling on `kafka_consumergroup_lag` is designed and documented (`backend/scaling-design.md`), but not deployed. Load testing showed zero sustained consumer lag under the current workload — a single consumer processes events faster than they arrive. Infrastructure is added when metrics justify it, not before.

---

## Project Structure

```
aegis-observability/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py              # FastAPI endpoints, live-path wiring, ML lifecycle endpoints, SPA static mount
│   │   ├── parser.py            # Structured key-value trace log parser
│   │   ├── correlation.py       # Propagation graph builder + topological RCA
│   │   ├── streaming.py         # StreamingCorrelator + IncidentAssembler
│   │   ├── predictor.py         # EWMA/OLS statistical failure predictor (trend, anomaly, error-rate)
│   │   ├── ml_predictor.py      # Runtime inference for the trained failure model (guarded, drift-aware)
│   │   ├── rca_ranker.py        # Runtime inference for the trained RCA ranker (guarded)
│   │   ├── incident_memory.py   # Incident similarity search (TF-IDF / sentence-transformers)
│   │   ├── model_registry.py    # Versioned model registry read/rollback helpers
│   │   ├── scoreboard.py        # Live STAT-vs-ML champion/challenger scoreboard
│   │   ├── drift.py             # PSI-based feature drift monitor
│   │   ├── simulator.py         # Simulated fleet + chaos fault injection
│   │   ├── live_state.py        # In-memory dashboard state registry
│   │   ├── pipeline.py          # Shared incident pipeline (batch + live paths, ML ranking + similarity attach)
│   │   ├── analyzer.py          # LLM integration with schema validation + deterministic fallback
│   │   ├── consumer.py          # Kafka consumer group (aiokafka)
│   │   ├── metrics.py           # Prometheus metric definitions
│   │   └── exporter.py          # War room artifact exporter
│   ├── ml/
│   │   ├── features.py          # Shared train/serve feature extraction (identity-free)
│   │   ├── generate_dataset.py  # Fake-clock simulator episode generation -> labeled CSVs
│   │   ├── train_failure_model.py   # Trains + versions the GBM failure model + IsolationForest comparison
│   │   ├── train_rca_ranker.py      # Trains + versions the RCA ranker
│   │   ├── build_incident_corpus.py # Builds the similarity-search corpus
│   │   ├── retrain_pipeline.py      # One-click retrain: fresh data -> train -> promotion gate
│   │   ├── artifacts/            # Versioned model artifacts + registry.json (committed to git)
│   │   └── data/                 # Generated CSV datasets (gitignored, regenerable)
│   ├── producers/
│   │   └── replay_producer.py   # HTTP + Kafka replay with jitter/late injection
│   ├── eval/
│   │   ├── ground_truth.json    # Expected root causes per scenario
│   │   ├── run_eval.py          # RCA + STAT lead-time + STAT-vs-ML evaluation harness
│   │   └── scorecard.md         # Latest evaluation results
│   ├── observability/
│   │   ├── prometheus.yml       # Prometheus scrape configuration
│   │   └── grafana/             # Auto-provisioned datasource + dashboard
│   ├── tests/                    # 69 tests across correlator, RCA, predictor, simulator, and the full ML layer
│   ├── sample_logs/
│   │   ├── redis_retry_storm.log
│   │   ├── pg_deadlock.log
│   │   └── cache_stampede.log
│   ├── .env.example              # GROQ_API_KEY template
│   ├── requirements.txt          # Base runtime deps (no ML)
│   ├── requirements-ml.txt       # numpy, pandas, scikit-learn — optional, guarded everywhere
│   └── scaling-design.md         # K8s/KEDA scaling design (gated on evidence)
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── ChaosPanel.jsx        # Fault injection controls
│   │   │   ├── IncidentFeed.jsx      # Expandable rows: ML ranking, agreement badge, similar incidents
│   │   │   ├── PredictionsPanel.jsx  # Hero: countdown, confidence, progress bar, ML/STAT badge
│   │   │   ├── ScoreboardStrip.jsx   # Live champion/challenger scoreboard
│   │   │   ├── ModelCard.jsx         # Version history, drift status, gated retrain button
│   │   │   ├── PropagationGraph.jsx  # RCA roles as SVG columns
│   │   │   ├── ServiceGrid.jsx       # Per-service status + sparklines
│   │   │   ├── Sparkline.jsx         # Hand-rolled SVG sparkline (no chart lib)
│   │   │   ├── PipelineMap.jsx       # Living architecture diagram, incl. ML Layer stage node
│   │   │   ├── DemoTimeline.jsx      # Auto-checking milestone tracker
│   │   │   ├── StatusStrip.jsx       # Infra health dots + totals
│   │   │   ├── GrafanaPanel.jsx      # Embedded Grafana iframe
│   │   │   └── ArtifactViewer.jsx    # War-room + scorecard viewer
│   │   ├── api.js                # Fetch helpers + polling hooks
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   └── styles.css
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── screenshots/
├── docker-compose.yml           # Kafka (KRaft) + Prometheus + Grafana
├── Dockerfile                   # Multi-stage: frontend build + backend runtime
├── .gitignore
└── README.md
```

---

## Testing

```bash
cd backend
pip install -r requirements.txt
pip install pytest locust
python -m pytest tests/ -v                    # 69 unit tests (all pass with or without ML deps installed)
python eval/run_eval.py                       # 3/3 RCA scorecard + STAT lead time + STAT-vs-ML comparison
locust -f tests/locustfile.py --headless \
  -u 20 -r 5 -t 60s                          # Load test

cd ../frontend
npm install && npm run build                  # Production frontend build
```

To exercise the ML layer specifically:
```bash
pip install -r requirements-ml.txt
python ml/generate_dataset.py && python ml/train_failure_model.py && python ml/train_rca_ranker.py
python -m pytest tests/test_ml_features.py tests/test_model_registry.py tests/test_scoreboard.py \
  tests/test_drift.py tests/test_incident_memory.py tests/test_ml_degradation.py -v
```

---

## FAQ

**How does the system handle out-of-order events?**
The streaming correlator maintains an event-time watermark calculated as `max_event_time_seen - grace_period`. Traces close only when the watermark advances past their latest event by the idle gap threshold. Events arriving after closure are counted as late (a quality signal) rather than silently dropped. This follows the same windowing model as Google Dataflow and Apache Beam.

**How is root cause distinguished from downstream symptoms?**
The call graph is reversed (failure propagates callee → caller, opposite to call direction) and topologically sorted using Kahn's algorithm. A degraded node with no degraded dependency beneath it is classified as `ROOT_CAUSE`; everything downstream is `SYMPTOM`. Cycles detected via Kahn's residual set or deadlock markers in `err_class` are classified as `CYCLE_MEMBER`. An optional trained ranker scores the same candidates independently and is shown alongside — see [The ML Lifecycle](#the-ml-lifecycle).

**How does the predictor avoid false positives?**
The statistical predictor: an R² ≥ 0.6 confidence floor on the trend fit, hysteresis re-arming, and dedup to one active prediction per `(service, metric, kind)`. The ML model: a precision-optimized decision threshold chosen at train time (≥90% precision on held-out episodes) plus the same TTL/dedup pattern, sharing the `Prediction` interface with the statistical predictor.

**Is the trained ML model actually better than the statistical baseline?**
No, and the README says so directly: on unseen simulated episodes, the statistical predictor gives ~228s of warning versus the ML model's ~117s — a direct, explainable consequence of the ML model's 120-second training label horizon, not a weaker model. See [The ML Lifecycle](#the-ml-lifecycle) for the full comparison and why it's not apples-to-apples to compare the ML model's capped number against the baseline's uncapped one without that context.

**What happens if I don't install the ML dependencies?**
Nothing breaks. Every ML-related module (`ml_predictor.py`, `rca_ranker.py`, `incident_memory.py`) guards its imports and returns `None`/disabled state when `numpy`/`scikit-learn`/`joblib` aren't present; the frontend renders those panels as unavailable with an install hint. This is directly tested in `tests/test_ml_degradation.py`.

**How does the retrain gate decide whether to promote a challenger?**
The failure model must not regress PR-AUC and must not lose more than 10% of median lead time versus the current champion; the RCA ranker must not regress top-1 accuracy. Both checks run in `ml/retrain_pipeline.py` against `metrics.json` snapshots saved at train time. A rejected challenger stays in the version history with its rejection reason recorded, visible in the Model Card.

**How does the dashboard show the Kafka pipeline if the consumer is a separate process?**
It reads the consumer through the consumer's own Prometheus metrics endpoint — the exact same interface Prometheus scrapes. No shared memory, no Redis, no push coupling: the dashboard's Pipeline Map polls `/kafka/stats`, which fetches and parses that exposition. If the consumer is down, the lane renders dimmed with the command to start it.

**Why does the in-browser Kafka replay require running the API on the host?**
The compose broker advertises `localhost:9092` (`KAFKA_ADVERTISED_LISTENERS`), so any *container* that connects gets told to reconnect to itself — only host processes can produce/consume. The `/kafka/replay` endpoint returns a clear 503 explaining this when the broker is unreachable.

**Could this predict failures hours in advance?**
The same EWMA/OLS technique generalizes to slower signals (disk fill-up, memory leaks) by downsampling to coarser time buckets before fitting the trend. It isn't implemented here because an hours-long ramp can't be demonstrated live.

**What happens if the LLM returns invalid output?**
The LLM response is validated against the `AegisDiagnosticReport` Pydantic schema using `model_validate`. On validation failure, JSON parse error, or API timeout, the system generates a complete diagnostic report from the deterministic RCA result. The LLM is never a single point of failure.

**Why isn't Kubernetes deployed?**
K8s + KEDA autoscaling on consumer lag is fully designed (`backend/scaling-design.md`) but intentionally not deployed. Load testing confirmed zero sustained consumer lag. The scaling design documents the exact conditions, KEDA configuration, and partition strategy for when production load justifies it.
