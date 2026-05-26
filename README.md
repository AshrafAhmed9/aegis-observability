# Aegis — AI-Native Incident Correlation & Observability Platform

Aegis is an observability-focused SRE incident correlation platform built for high-performance distributed systems. It couples a **Deterministic Telemetry Correlation Engine** with structured AI diagnostics to eliminate the high cognitive load associated with analyzing complex microservice cascades.

Unlike shallow, raw AI chatbot wrappers that attempt to directly analyze chaotic, unstructured logs (leading to untrustworthy hallucinations), Aegis implements a strict **Operational Trust Boundary**. The platform performs deterministic telemetry parsing and graph modeling *first*, establishing a solid mathematical ground truth. The AI layer operates strictly *second*, interpreting the compiled graphs, prioritizing hypotheses, and synthesizing code-level patches.

---

## 🏗️ System Architecture & Telemetry Pipeline

Aegis is designed around a decoupled, high-performance telemetry correlation pipeline:

```text
       +---------------------------------------------+
       |           Raw Unstructured Logs             |
       +---------------------------------------------+
                              │
                              ▼
       +---------------------------------------------+
       |          Structured Parsing Layer           |
       |  (Regex Key-Value & Trace Token Extraction)  |
       +---------------------------------------------+
                              │
                              ▼
       +---------------------------------------------+
       |      Telemetry Correlation Engine           |
       |  (Deterministic trace/span grouping)        |
       +---------------------------------------------+
                              │
                              ▼
       +---------------------------------------------+
       |       Dependency Graph Construction         |
       |  (Failure Propagation & Blast Radius Calc)  |
       +---------------------------------------------+
                              │
                              ▼  [Operational Trust Boundary]
       + - - - - - - - - - - - - - - - - - - - - - - +
       :             AI Augmentation Layer           :
       :  (Pydantic Structured Diagnostic Synthesis) :
       + - - - - - - - - - - - - - - - - - - - - - - +
                              │
                              ▼
       +---------------------------------------------+
       |        Jetro Spatial Whiteboard Cockpit     |
       |  (Markdown RCA, Flowcharts, CSV, Diffs)     |
       +---------------------------------------------+
```

---

## 🛠️ Deep Technical Specifications

### 1. Ingestion & Logging Realism
Aegis parses structured logs carrying rich distributed trace metadata. Ingested telemetry includes operational SRE tracking fields:
* `trace_id`: Hexadecimal transaction request tracker.
* `span_id` / `parent_span_id`: Direct transaction node relationships.
* `service_name`: Identifier of the executing microservice.
* `latency_ms`: Duration metrics showing database/cache delays.
* `connection_pool_usage`: Active database connection utilization levels.
* `queue_depth`: Task count indicators for queue backpressure modeling.

Example Log Line:
```text
2026-05-28T11:42:13Z service=celery-worker trace_id=tr-98b24 span_id=sp-017 task_name=tasks.process_payment task_id=cel_0981e queue_depth=842 retry_count=9 connection_pool_usage=100% severity=CRITICAL msg="Celery thread pool starvation. Worker thread limits reached. Crash loop initiated."
```

### 2. Deterministic Telemetry Correlation Engine
The engine analyzes standard transaction logs, associates events using their shared `trace_id`, and builds a chronological dependency graph. The parent-child span linkages map out the **Failure Propagation Path** across system nodes, identifying where latencies started and where timeouts cascaded.

### 3. Pydantic-Validated Structured AI RCA
The AI layer intercepts the output of the correlation engine and returns a strictly typed JSON structure validated by Pydantic:
* **Blast Radius Assessment:** Identifies degraded nodes and estimates affected user checkouts.
* **Ranked Hypotheses:** Evaluates and rates possible failure causes with specific confidence percentages.
* **Remediation Code Patches:** Synthesizes actual, unified git diff code patches ready for developer review.

---

## 💻 Visual Spatial Dashboard (Jetro Workspace Integration)

Aegis publishes its diagnostic reports directly into the `active_war_room/` workspace folder, enabling **Jetro** to display a highly collaborative, spatial "Incident War Room":

* **`incident_summary.md`**: Multi-hypothesis root cause analysis and severity indicators.
* **`incident_timeline.md`**: An easy-to-read trace timeline highlighting error propagation times.
* **`incident_graph.md`**: A visual, color-coded Mermaid flow diagram showing precisely what failed.
* **`postmortem.md`**: An SRE-grade operational postmortem with concrete prevention tasks.
* **`telemetry_db.csv`**: A CSV database of the traces that developers can **query directly using SQL inside Jetro**!
* **`suggested_patch.diff`**: A ready-to-merge code fix showing the recommended configuration patch.

---

## 🎯 Production Failure Scenarios Covered

Aegis is loaded with three prepackaged, highly realistic distributed system incident traces:
1. **Redis Pool Exhaustion & Celery Retry Storm (`redis_retry_storm.log`):** Demonstrates connection limit exhaustion cascading into worker thread starvation and infinite retry task backlogs.
2. **PostgreSQL Deadlock (`pg_deadlock.log`):** Shows parallel transactions acquiring exclusive row locks in reversed order, causing transaction rollbacks.
3. **Cache Stampede (`cache_stampede.log`):** Simulates a rate-limiter bypass coupled with expired popular keys, flooding the primary database with concurrent join operations.

---

## 🚀 Running Aegis Locally

### 1. Ingest Telemetry
1. Open the folder `C:\Users\ashra\.gemini\antigravity\scratch\aegis` in your editor.
2. Double-click `backend/run.bat` (or run `python -m uvicorn app.main:app --reload` inside `backend/`).
3. Send a POST request to `/ingest` using curl or Postman to analyze a scenario:
   ```bash
   curl -X POST http://127.0.0.1:8000/ingest -H "Content-Type: application/json" -d '{"log_filename": "redis_retry_storm.log"}'
   ```

### 2. Access the Jetro War Room
1. In your **Antigravity IDE**, open the **Jetro Workspace**.
2. Open the files under `active_war_room/` to view the generated incident cards, timelines, and Mermaid flowcharts.
3. Open `telemetry_db.csv` inside Jetro and query the logs dynamically using standard SQL queries!
