# Aegis — Predictive Observability Platform

> **Streaming telemetry correlation engine with event-time watermarks, topological root-cause analysis, and a shadow-mode failure predictor for distributed systems.**

[![Python](https://img.shields.io/badge/Python-3.12-3776ab?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![scikit--learn](https://img.shields.io/badge/scikit--learn-1.5-f7931e?style=flat&logo=scikitlearn&logoColor=white)](https://scikit-learn.org)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231f20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Prometheus](https://img.shields.io/badge/Prometheus-Instrumented-e6522c?style=flat&logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboard-f46800?style=flat&logo=grafana&logoColor=white)](https://grafana.com)
[![Tests](https://img.shields.io/badge/Tests-65%20passing-brightgreen?style=flat)]()
[![CI](https://github.com/AshrafAhmed9/aegis-observability/actions/workflows/ci.yml/badge.svg)](https://github.com/AshrafAhmed9/aegis-observability/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## The problem

When a distributed system breaks, the service making the most noise is usually not the one that
broke.

A database deadlocks. The API in front of it starts timing out and floods your alerts with
CRITICAL errors, because it's the one facing users. The database logs a single line and goes
quiet. At 3am, an on-call engineer is staring at hundreds of errors from a service that is
working fine, trying to work out which of the screaming things actually started it.

Aegis answers that question automatically.

## The core idea

Root-cause analysis is treated as a **graph problem, not a scoring problem**.

Log lines aren't independent. When one service calls another, the downstream span records the
identity of the request that triggered it. That's standard distributed tracing, modelled exactly
as OpenTelemetry does it. Follow every one of those parent pointers and a pile of alerts becomes
a directed graph of what caused what.

On that graph, "which one started it" has an exact answer: **the node with no incoming edges.**

```mermaid
flowchart LR
    DB["<b>database</b><br/>1 × ERROR<br/><i>postgres.deadlock</i>"] --> API["<b>api</b><br/>4 × CRITICAL<br/><i>OperationalError</i>"]

    style DB fill:#7f1d1d,stroke:#ef4444,stroke-width:3px,color:#fff
    style API fill:#78350f,stroke:#f59e0b,stroke-width:2px,color:#fff
```

`api` is louder by 4× and it is the *effect*. `database` has nothing pointing at it, so it's the
cause. A topological sort (Kahn's algorithm) generalises this to arbitrarily deep dependency
chains, and it's **correct on all 78 incidents in the evaluation set**.

Everything else in the project exists to build that graph, read it, or warn before it forms.

## Why the ML model doesn't decide anything

There is a gradient-boosting model here, and **it is not permitted to choose the root cause.**
The graph chooses it.

The model runs in shadow mode, predicting which service is about to fail. Its scores are
recorded and evaluated, never acted upon.

That boundary is deliberate. The deterministic engine is verifiable against known ground truth,
78 times out of 78. The model is not. Information flows *into* the advisory layer and never back
out into the decision, so the worst the model can do is be unhelpful.

---

## Architecture

Five layers. An event enters at the top and moves straight down; nothing skips a layer and
nothing flows back up.

```mermaid
flowchart TD
    ING["① INGESTION<br/><b>simulator.py</b> + <b>kafka_io.py</b><br/><i>events onto Kafka</i>"]
    KAFKA{{"Kafka · topic: telemetry.raw"}}
    STR["② STREAM &nbsp;·&nbsp; the only stateful layer<br/><b>correlator.py</b><br/><i>buffers per trace, closes on event-time watermarks</i>"]
    DEC["③ DECISION &nbsp;·&nbsp; system of record<br/><b>rca.py</b><br/><i>dependency graph + Kahn's sort → root cause</i>"]
    ADV["④ ADVISORY &nbsp;·&nbsp; no authority over the diagnosis<br/><b>ml.py</b> risk scoring &amp; drift"]
    SRV["⑤ SERVING<br/><b>main.py</b> · FastAPI + Prometheus metrics"]

    ING --> KAFKA --> STR
    STR -->|one sealed trace| DEC
    DEC -->|the answer| ADV
    ADV --> SRV

    style DEC fill:#0c4a6e,stroke:#38bdf8,stroke-width:3px,color:#fff
    style KAFKA fill:#1e293b,stroke:#94a3b8,color:#fff
    style ING fill:#1e293b,stroke:#64748b,color:#fff
    style STR fill:#1e293b,stroke:#64748b,color:#fff
    style ADV fill:#1e293b,stroke:#64748b,color:#fff
    style SRV fill:#1e293b,stroke:#64748b,color:#fff
```

Two files sit outside this flow because they never run during a request: `train.py` produces the
model, and `run_eval.py` grades the pipeline.

### How each layer works

**Ingestion.** `simulator.py` generates fault episodes for a four-service fleet: a cache leak, a
queue backlog, and a database deadlock burst. Each episode has a known culprit, a realistic retry
backoff before failure, and a cascade whose spans point back at the original error. Swapping this
for a real OTLP collector would change nothing downstream, since the event shape is already the
OpenTelemetry span model.

**Stream.** Events arrive out of order and nothing ever signals that a trace is complete.
`correlator.py` buffers them per `trace_id` and closes a trace on an **event-time watermark**,
the newest timestamp seen minus a grace period, so network delays can't change the analysis. A
max-age rule bounds memory, and a wall-clock idle rule handles the case where no further traffic
exists to advance the watermark.

**Decision.** `rca.py` builds one node per service and one edge per `parent_span_id` crossing a
service boundary, then runs Kahn's algorithm over the failed services. It handles the awkward
cases explicitly: services in a dependency cycle can't be ordered, so all of them are reported as
causes, and failures with no causal edge between them fall back to earliest-error ordering, which
is the weaker path and is labelled as such.

**Advisory.** `ml.py` reduces each event to four features. The dominant one is how long a
service has been silent: a healthy one checks in every five seconds, a struggling one goes quiet.
Features come only from prior events, so no future information leaks into training. A PSI drift
monitor over a rolling window reports whether live traffic still resembles what the model learned
from.

**Serving.** `main.py` exposes the API and runs two background tasks: the Kafka consumer, and a
loop that closes traces gone quiet in real time. `/incidents` returns the root-cause results,
`/predictions` the live shadow scores, and `/metrics` feeds Prometheus.

---

## Results

Every number below is recomputed from scratch on each run, and re-verified by CI on every push.

| Metric | Result | How it's measured |
|---|---|---|
| Root-cause accuracy | **78 / 78** | 26 episodes × 3 fault types, run blind through the real pipeline |
| Median early warning | **193.4s** | First high-risk prediction → actual failure, across 18 held-out episodes |
| Prediction recall | **18 / 18** | Held-out episodes where the model raised an alarm before failure |
| Test suite | **65 passing** | Correlation, RCA, prediction, drift, simulation |

```bash
cd backend
python train.py      # trains the model, prints the measured lead time
python run_eval.py   # grades the correlation engine, writes scorecard.md
```

`run_eval.py` regenerates all 78 episodes, feeds each through an actual `Correlator` and
`rca.analyze`, the same classes that serve live traffic rather than a test double, then compares
the predicted root cause against the injected one. Misses are written to `scorecard.md` with
their seeds, so any failure reproduces exactly.

**On the benchmark being synthetic:** it is, deliberately, and that's what makes the number
meaningful. Real production logs carry no ground truth, so an engine pointed at them can't be
scored at all. Generating the faults means the correct answer is known before the engine sees the
data. This validates the correlation and ranking logic; it does not claim to have survived the
mess of real production telemetry.

---

## Quick start

```bash
git clone https://github.com/AshrafAhmed9/aegis-observability.git
cd aegis-observability

docker-compose up -d kafka        # the API needs Kafka running

cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python train.py
uvicorn main:app --port 8010
```

Inject a fault and read the result:

```bash
curl -X POST http://127.0.0.1:8010/simulate/redis_leak
curl http://127.0.0.1:8010/incidents      # a few seconds later
```

Once the virtualenv is set up and `train.py` has run, **`./demo.sh`** does all of the above in
one command. It starts everything, simulates an outage, prints which service Aegis blamed and
why, and opens the Grafana dashboard and API docs in your browser.

### Dashboards

To also get Prometheus and Grafana, start all three instead of just Kafka:

```bash
docker-compose up -d kafka prometheus grafana
```

| Service | URL |
|---|---|
| API | http://127.0.0.1:8010 |
| API docs | http://127.0.0.1:8010/docs |
| Prometheus | http://localhost:9091 |
| Grafana | http://localhost:3000 (admin / aegis) |

---

## Project layout

```
backend/
  simulator.py       synthetic fault episodes (demo traffic + eval data)
  correlator.py      event-time watermark buffering
  rca.py             dependency graph + Kahn's topological sort
  ml.py              feature extraction, GBM shadow detector, PSI drift
  kafka_io.py        Kafka producer/consumer
  main.py            FastAPI app, background tasks, Prometheus metrics
  train.py           trains the model on simulator-generated data
  run_eval.py        78-episode correctness + lead-time evaluation
  tests/             65 tests
  observability/     Prometheus + Grafana provisioning
```

```bash
cd backend && pytest      # 65 tests, runs in about a second
```

Tests are fast because event timestamps are plain numbers rather than wall-clock time. A test
for a 320-second closing rule doesn't wait 320 seconds.

## License

MIT
