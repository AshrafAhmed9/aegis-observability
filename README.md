# Aegis — AI-Native Incident Correlation & Observability Platform

Aegis is an observability-focused SRE incident correlation platform built for high-performance distributed systems. It couples a **Deterministic Telemetry Correlation Engine** with structured AI diagnostics to eliminate the high cognitive load associated with analyzing complex microservice cascades.

Unlike shallow AI chatbot wrappers that attempt to directly analyze chaotic, unstructured logs, Aegis implements a strict **Operational Trust Boundary**. The platform performs deterministic telemetry parsing and graph modeling *first*, establishing a solid mathematical ground truth. The AI layer (powered by **Groq LLaMA 3.3 70B**) operates strictly *second*, interpreting compiled graphs, prioritizing hypotheses, and synthesizing code-level patches.

---

## 📸 Screenshots

### Jetro Incident War Room
![Jetro War Room](screenshots/jetro_warroom.png)

### Failure Propagation Graph
![Failure Propagation Graph](screenshots/jetro_graph.png)

### API — `/scenarios` Endpoint
![Swagger Scenarios](screenshots/swagger_scenarios.png)

### API — `/ingest` Endpoint (Live AI RCA)
![Swagger Ingest](screenshots/swagger_ingest.png)

---

## 🏗️ System Architecture & Telemetry Pipeline

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
       :       Groq LLaMA 3.3 70B AI RCA Layer       :
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
Aegis parses structured logs carrying rich distributed trace metadata:
* `trace_id`: Hexadecimal transaction request tracker
* `span_id` / `parent_span_id`: Direct transaction node relationships
* `service_name`: Identifier of the executing microservice
* `latency_ms`: Duration metrics showing database/cache delays
* `connection_pool_usage`: Active database connection utilization levels
* `queue_depth`: Task count indicators for queue backpressure modeling

Example Log Line:
```text
2026-05-28T11:42:13Z service=celery-worker trace_id=tr-98b24 span_id=sp-017 task_name=tasks.process_payment queue_depth=842 retry_count=9 connection_pool_usage=100% severity=CRITICAL msg="Celery thread pool starvation. Worker thread limits reached. Crash loop initiated."
```

### 2. Deterministic Telemetry Correlation Engine
The engine analyzes transaction logs, associates events using shared `trace_id`, and builds a chronological dependency graph. Parent-child span linkages map out the **Failure Propagation Path** across system nodes, identifying where latencies started and where timeouts cascaded.

### 3. Groq AI-Powered Structured RCA
The AI layer intercepts the correlation engine output and returns a strictly typed JSON structure validated by Pydantic v2:
* **Blast Radius Assessment:** Identifies degraded nodes and estimates affected user requests
* **Ranked Hypotheses:** Evaluates possible failure causes with confidence percentages
* **Remediation Code Patches:** Synthesizes unified git diff patches ready for developer review
* **Graceful Fallback:** Falls back to pre-packaged offline diagnostics if API is unavailable

---

## 💻 Visual Spatial Dashboard (Jetro Workspace Integration)

Aegis publishes diagnostic reports into `active_war_room/`, enabling Jetro to display a collaborative spatial Incident War Room:

| File | Contents |
| :--- | :--- |
| `incident_summary.md` | Multi-hypothesis RCA and severity indicators |
| `incident_timeline.md` | Trace timeline highlighting error propagation |
| `incident_graph.md` | Color-coded Mermaid failure propagation flowchart |
| `postmortem.md` | SRE-grade postmortem with prevention action items |
| `telemetry_db.csv` | SQL-queryable trace database |
| `suggested_patch.diff` | Ready-to-merge unified git diff code fix |

---

## 🎯 Production Failure Scenarios

| Scenario | Log File | Domain |
| :--- | :--- | :--- |
| Redis Pool Exhaustion & Celery Retry Storm | `redis_retry_storm.log` | Distributed Queuing, Backpressure |
| PostgreSQL Row Lock Deadlock | `pg_deadlock.log` | Database Transactions, Race Conditions |
| Cache Stampede & DB Starvation | `cache_stampede.log` | Caching, High-Throughput I/O |

---

## 🚀 Running Locally

### Prerequisites
- Python 3.8+
- A free [Groq API key](https://console.groq.com) (optional — falls back to offline diagnostics without it)

### Setup

```bash
cd backend
```

Create a `.env` file in the `backend/` folder:
```
GROQ_API_KEY=your_groq_api_key_here
```

Run the server:
```bash
.\run.bat
```

### Test the API

```bash
# List available scenarios
curl http://127.0.0.1:8000/scenarios

# Ingest a log and generate AI RCA
curl -X POST http://127.0.0.1:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"log_filename": "redis_retry_storm.log"}'
```

Open `http://127.0.0.1:8000/docs` for the interactive Swagger UI.

---

## 📁 Project Structure

```
aegis-observability/
├── app/
│   ├── main.py              # FastAPI endpoints (/ingest, /scenarios)
│   ├── parser.py            # Structured KV trace log parser
│   ├── correlation.py       # Deterministic trace correlation & graph builder
│   ├── analyzer.py          # Groq AI RCA engine (Pydantic v2 schemas)
│   └── jetro_service.py     # War room artifact exporter
├── sample_logs/
│   ├── redis_retry_storm.log
│   ├── pg_deadlock.log
│   └── cache_stampede.log
├── requirements.txt
└── run.bat
```
