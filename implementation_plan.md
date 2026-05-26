# Aegis — AI-Native Incident Correlation & Observability Platform

Aegis is an observability-focused platform built on a hybrid architecture of deterministic systems telemetry correlation and structured AI root-cause analysis (RCA) for distributed systems. 

Aegis consumes raw distributed logs containing trace and request telemetry, correlates them through a deterministic pipeline, models incident propagation graphs across microservices, and publishes a highly operational, collaborative **"Incident War Room"** spatial dashboard directly to the **Jetro Workspace**.

---

## 🛡️ The Aegis Ingestion & Diagnostics Pipeline

To ensure absolute reliability (preventing LLM hallucination and raw guessing), Aegis operates a rigorous multi-stage systems engineering pipeline:

```text
Raw Logs with Trace IDs 
  ➔ [Parser] ➔ Structured Telemetry Events
  ➔ [Telemetry Correlation Engine] ➔ Deterministic Chronological Timeline
  ➔ [Incident Graph Builder] ➔ Failure Propagation Graph (Service Dependencies)
  ➔ [AI RCA Augmentor] ➔ Structured Multi-Hypothesis Diagnostic Engine
  ➔ [Postmortem Generator] ➔ Dynamic Jetro Workspace Whiteboard Cockpit
```

### 1. Operational Trust Boundary
Aegis enforces a strict trust boundary: **LLMs are never trusted as the source of truth for telemetry correlation.**
Instead, the deterministic correlation engine processes raw Trace IDs, request identifiers, and system metrics to establish mathematical ground truth. The AI layer acts strictly as an *augmentor*, interpreting the compiled failure graph, prioritizing systems hypotheses, and synthesizing code-level remediation patches.

### 2. Failure Propagation Graph Modeling
Distributed failures propagate along service dependency paths. Aegis builds directed graphs representing this execution cascade:
```text
postgres-db (Lock Contention) 
  ➔ redis-cache (Connection Pool Exhausted) 
  ➔ celery-worker (Task Timeout & Retry Storm) 
  ➔ api-gateway (HTTP 504 Gateway Timeout)
```
By tracing parent-child span relationships, Aegis estimates the **Blast Radius** (estimated affected checkout requests and degraded services) and visualizes the propagation chain inside Jetro.

---

## 🎯 Canonical Failure Scenario: Redis Pool Exhaustion & Retry Storm

Aegis prioritizes a deeply polished, highly realistic telemetry trace to showcase backend engineering depth:

* **Trigger event:** Database lock contention causes transaction latencies to spike.
* **Secondary propagation:** Downstream client processes attempt high-frequency cache operations, saturating the Redis connection pool.
* **Tertiary cascade:** Celery worker task timeouts trigger a task retry storm. Lacking jittered exponential backoff, workers enter tight synchronization loops, causing thread pool starvation and crash loops.
* **System-wide impact:** Checkout endpoints fail with gateway timeouts (`HTTP 504`), stalling all active user checkout flows.

---

## 📁 Project Structure

```text
C:\Visual studio PROJECTS\JETRO/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py            # FastAPI Application & Endpoints
│   │   ├── parser.py          # Structured Telemetry Parser (Trace IDs)
│   │   ├── correlation.py     # Deterministic Correlation & Graph Builder
│   │   ├── analyzer.py        # Pydantic Structured AI RCA & Hypothesis Engine
│   │   └── jetro_service.py   # Jetro File-Backed Board Exporter
│   ├── sample_logs/           # High-fidelity distributed log telemetry
│   │   ├── redis_retry_storm.log
│   │   ├── pg_deadlock.log
│   │   └── cache_stampede.log
│   ├── requirements.txt
│   └── run.bat                # Backend bootstrap script
├── active_war_room/           # Sibling folder mapped directly to Jetro Canvas
│   ├── incident_summary.md    # Multi-Hypothesis Diagnostic Card
│   ├── incident_timeline.md   # Chronological Trace Lifecycle
│   ├── incident_graph.md      # Mermaid Failure Propagation Chart
│   ├── postmortem.md          # SRE Postmortem RFC Document
│   ├── telemetry_db.csv       # SQL-Queryable CSV trace database in Jetro
│   └── suggested_patch.diff   # Remediation unified git diff patch
├── README.md                  # SRE-grade system documentation & RFC
└── architecture.mermaid       # Pipeline diagram source
```

---

## 📊 Structured Output Models (`app/analyzer.py`)

Aegis enforces typing and structure throughout the pipeline using strict Pydantic v2 schemas:

```python
from pydantic import BaseModel, Field
from typing import List

class FailureNode(BaseModel):
    service_name: str = Field(description="Name of the service containing the node")
    error_class: str = Field(description="Exception or error type")
    severity: str = Field(description="CRITICAL, ERROR, WARNING")
    latency_ms: float = Field(description="Duration or execution time")

class IncidentGraphEdge(BaseModel):
    source_service: str = Field(description="Service initiating the dependency failure")
    target_service: str = Field(description="Downstream service affected by the failure")
    propagation_type: str = Field(description="e.g. TIMEOUT, DEADLOCK, BACKPRESSURE")

class Hypothesis(BaseModel):
    root_cause: str = Field(description="Systems-level description of hypothetical root failure")
    confidence: float = Field(description="Confidence percentage (0.0 to 1.0)")
    description: str = Field(description="Technical rationale for this hypothesis")

class CodeRemediation(BaseModel):
    file_path: str = Field(description="Remediation file path")
    explanation: str = Field(description="Technical explanation of the proposed fix")
    git_diff: str = Field(description="Unified git diff file showing recommended patch")

class AegisDiagnosticReport(BaseModel):
    incident_id: str
    title: str = Field(description="SRE Incident Code (e.g. INC-2026-REDIS)")
    overall_severity: str = Field(description="CRITICAL, HIGH, MEDIUM, LOW")
    impact_analysis: str = Field(description="Systems-level business and database impact summary")
    hypotheses: List[Hypothesis] = Field(description="Ranked possible root causes")
    primary_remediation: CodeRemediation
```

---

## 🔍 Verification Plan

### 1. Verification of the Ingestion Pipeline
- Process each of the three sample logs to confirm they execute end-to-end:
  - Parses events, extracts trace ids (`trace_id`, `request_id`, etc.).
  - Builds the deterministic timeline and failure propagation paths.
  - LLM successfully yields a Pydantic-validated `AegisDiagnosticReport`.
  - Exporters successfully write the entire suite of `active_war_room/` files.

### 2. Manual Visual Workspace Verification in Jetro
- Launch **Antigravity IDE** in the project folder.
- Execute ingestion via backend endpoints.
- Open **Jetro Workspace** in the editor:
  - Verify that the `incident_summary.md` and `postmortem.md` load cleanly as rich cards.
  - Verify that the Mermaid `incident_graph.md` renders the propagation path correctly.
  - Query `telemetry_db.csv` inside Jetro with standard SQL commands to prove data observability.
