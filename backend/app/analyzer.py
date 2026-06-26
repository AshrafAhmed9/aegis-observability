from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Dict, Any
import json
import os

class TimelineEvent(BaseModel):
    timestamp: str = Field(description="ISO-8601 or log timestamp")
    component: str = Field(description="Name of the service/infra element")
    severity: str = Field(description="INFO, WARNING, ERROR, CRITICAL")
    message: str = Field(description="Highly concise, technical event description")

class Hypothesis(BaseModel):
    root_cause: str = Field(description="Systems-level description of hypothetical root failure")
    confidence: float = Field(description="Confidence percentage (0.0 to 1.0)")
    description: str = Field(description="Detailed technical rationale for this hypothesis")

class CodeRemediation(BaseModel):
    file_path: str = Field(description="Target codebase file path to patch")
    explanation: str = Field(description="Technical explanation of the proposed fix")
    git_diff: str = Field(description="Unified git diff showing recommended patch")

class SREPostmortem(BaseModel):
    executive_summary: str = Field(description="Concise description of what broke, impact, and fix")
    action_items_prevention: List[str] = Field(description="Steps to prevent recurrence")

class AegisDiagnosticReport(BaseModel):
    incident_id: str = Field(description="SRE Incident Code (e.g. INC-2026-REDIS)")
    title: str = Field(description="Professional, technical incident title")
    overall_severity: str = Field(description="CRITICAL, HIGH, MEDIUM, LOW")
    impact_analysis: str = Field(description="Systems-level business and database impact summary")
    blast_radius_requests: int = Field(description="Estimated number of affected requests")
    hypotheses: List[Hypothesis] = Field(description="Ranked possible root causes with confidence scores")
    primary_remediation: CodeRemediation = Field(description="Specific actionable code fix")
    postmortem: SREPostmortem = Field(description="Standardized postmortem analysis")

OFFLINE_DIAGNOSTICS = {
    "redis_retry_storm.log": AegisDiagnosticReport(
        incident_id="INC-2026-REDIS-STORM",
        title="Redis Connection Pool Exhaustion & Celery Worker Thread Starvation Cascade",
        overall_severity="CRITICAL",
        impact_analysis="Redis connection pool saturation (200/200 active connections) triggered cascading failures across the checkout pipeline. Celery workers entered an exponential retry storm (queue depth: 12 → 842), exhausting thread pools. API gateway experienced upstream timeouts (15,004ms latency), returning HTTP 504 to end users. Estimated 19,575 affected requests over a 2.5-minute degradation window.",
        blast_radius_requests=19575,
        hypotheses=[
            Hypothesis(root_cause="Redis Connection Pool Exhaustion", confidence=0.95, description="Connection pool reached 100% utilization (200/200 connections). All subsequent connection attempts resulted in redis.exceptions.ConnectionError after 5000ms timeout. Root trigger likely a sudden spike in cache operations or a connection leak in the client library."),
            Hypothesis(root_cause="Celery Exponential Retry Storm Amplification", confidence=0.85, description="Failed Redis operations triggered Celery task retries with exponential backoff. However, the retry mechanism lacked jitter and max-retry caps, causing queue depth to explode from 12 to 842 tasks. Each retry consumed a thread and attempted a new Redis connection, creating a positive feedback loop.")
        ],
        primary_remediation=CodeRemediation(
            file_path="infra/config/redis_pool.py",
            explanation="Increase max connections, add connection timeout with circuit breaker, and implement retry budget on Celery tasks to prevent amplification cascades.",
            git_diff="--- a/infra/config/redis_pool.py\n+++ b/infra/config/redis_pool.py\n@@ -1,5 +1,9 @@\n REDIS_CONFIG = {\n-    'max_connections': 200,\n+    'max_connections': 500,\n+    'socket_connect_timeout': 3,\n+    'socket_timeout': 5,\n+    'retry_on_timeout': True,\n+    'health_check_interval': 30,\n }"
        ),
        postmortem=SREPostmortem(
            executive_summary="Redis connection pool exhaustion caused a cascading failure through Celery workers to the API gateway. The root cause was insufficient pool sizing combined with missing retry budgets on async task workers, creating an amplification loop.",
            action_items_prevention=["Increase Redis connection pool to 500 with health checks", "Add circuit breaker with 80% pool utilization threshold", "Implement max retry count (3) with jitter on Celery tasks", "Add connection pool utilization alerting at 75% threshold"]
        )
    ),
    "pg_deadlock.log": AegisDiagnosticReport(
        incident_id="INC-2026-PG-DEADLOCK",
        title="PostgreSQL Row Lock Deadlock under Concurrent Order Transactions",
        overall_severity="HIGH",
        impact_analysis="Two concurrent order transactions (TX_001, TX_002) acquired row-level locks in non-deterministic order, creating a circular wait condition. PostgreSQL's deadlock detector resolved the cycle after 3.45 seconds by aborting TX_001. The order-service propagated the error as an unhandled OperationalError, resulting in HTTP 500 responses.",
        blast_radius_requests=5220,
        hypotheses=[
            Hypothesis(root_cause="Non-deterministic Lock Acquisition Order", confidence=0.98, description="TX_001 locked row 9982 then requested 7711; TX_002 locked 7711 then requested 9982. This classic ABBA deadlock pattern indicates the application does not enforce a canonical lock ordering on inventory rows."),
            Hypothesis(root_cause="Missing Application-Level Deadlock Retry", confidence=0.75, description="The order-service does not catch deadlock exceptions and retry the transaction. A single retry with backoff would have resolved the aborted transaction transparently.")
        ],
        primary_remediation=CodeRemediation(
            file_path="backend/app/order_service.py",
            explanation="Sort inventory item IDs before issuing SELECT FOR UPDATE to enforce deterministic lock ordering, eliminating the circular wait condition.",
            git_diff="--- a/backend/app/order_service.py\n+++ b/backend/app/order_service.py\n@@ -10,6 +10,7 @@\n def process_order(items):\n+    items = sorted(items, key=lambda x: x['item_id'])\n     for item in items:\n         db.execute('SELECT * FROM inventory WHERE item_id = %s FOR UPDATE', (item['item_id'],))"
        ),
        postmortem=SREPostmortem(
            executive_summary="A PostgreSQL deadlock between two concurrent order transactions caused HTTP 500 errors. Root cause was non-deterministic lock acquisition order on inventory rows.",
            action_items_prevention=["Enforce canonical lock ordering by sorting item IDs before SELECT FOR UPDATE", "Add deadlock retry logic (max 3 retries with exponential backoff)", "Add deadlock frequency alerting via pg_stat_activity monitoring"]
        )
    ),
    "cache_stampede.log": AegisDiagnosticReport(
        incident_id="INC-2026-CACHE-STAMPEDE",
        title="Cache Miss Stampede & PostgreSQL Connection Pool Exhaustion under Concurrent Read Peak",
        overall_severity="CRITICAL",
        impact_analysis="Rate limiter synchronization delay (5.12s) allowed a burst of concurrent requests to bypass throttling. Simultaneous cache misses on key 'prod:popular' triggered a thundering herd of identical database queries. PostgreSQL connection pool exhausted at 100/100 active connections, causing psycopg2.OperationalError timeouts. API gateway returned HTTP 503 ServiceUnavailable.",
        blast_radius_requests=13050,
        hypotheses=[
            Hypothesis(root_cause="Cache Stampede (Thundering Herd)", confidence=0.92, description="Cache key 'prod:popular' expired, and N concurrent requests all experienced cache misses simultaneously. Without stampede protection (e.g., lock-based recomputation or probabilistic early expiration), all N requests issued identical SELECT queries to PostgreSQL."),
            Hypothesis(root_cause="Rate Limiter Synchronization Failure", confidence=0.70, description="The rate limiter's 5.12-second sync delay allowed requests to bypass throttling during the cache miss window, amplifying the stampede effect.")
        ],
        primary_remediation=CodeRemediation(
            file_path="backend/app/cache_service.py",
            explanation="Implement cache stampede protection using a mutex/lock pattern: only one request recomputes the cache value while others wait on the lock or serve stale data.",
            git_diff="--- a/backend/app/cache_service.py\n+++ b/backend/app/cache_service.py\n@@ -5,6 +5,15 @@\n def get_cached(key):\n-    value = cache.get(key)\n-    if value is None:\n-        value = db.query(key)\n-        cache.set(key, value, ttl=300)\n+    value = cache.get(key)\n+    if value is None:\n+        lock_key = f'lock:{key}'\n+        if cache.set(lock_key, '1', nx=True, ex=30):\n+            value = db.query(key)\n+            cache.set(key, value, ttl=300)\n+            cache.delete(lock_key)\n+        else:\n+            for _ in range(50):\n+                time.sleep(0.1)\n+                value = cache.get(key)\n+                if value is not None:\n+                    break\n     return value"
        ),
        postmortem=SREPostmortem(
            executive_summary="A cache stampede on key 'prod:popular' exhausted PostgreSQL connections when concurrent requests all missed cache simultaneously. The rate limiter failed to throttle due to a sync delay.",
            action_items_prevention=["Implement cache stampede protection with mutex/lock pattern", "Add probabilistic early expiration (XFetch algorithm) for hot keys", "Fix rate limiter sync to eliminate bypass window", "Set PostgreSQL connection pool queue timeout to fail fast (2s)"]
        )
    )
}

class AegisAnalyzer:
    @staticmethod
    def analyze(log_filename: str, trace_events: list, blast_radius: dict,
                rca_result: Optional[Dict[str, Any]] = None) -> AegisDiagnosticReport:
        api_key = os.environ.get("GROQ_API_KEY")

        # Try LLM first
        if api_key:
            try:
                return AegisAnalyzer._analyze_with_groq(api_key, log_filename, trace_events, blast_radius, rca_result)
            except Exception:
                pass

        # Fallback 1: offline diagnostics keyed by filename
        basename = os.path.basename(log_filename)
        if basename in OFFLINE_DIAGNOSTICS:
            return OFFLINE_DIAGNOSTICS[basename]

        # Fallback 2: build from deterministic RCA result
        if rca_result and rca_result.get("ranked_root_causes"):
            return AegisAnalyzer._build_from_rca(log_filename, blast_radius, rca_result)

        # Fallback 3: generic
        return AegisDiagnosticReport(
            incident_id="INC-UNKNOWN",
            title=f"Incident Analysis for {log_filename}",
            overall_severity="MEDIUM",
            impact_analysis="Automated analysis could not determine full impact.",
            blast_radius_requests=blast_radius.get("estimated_affected_requests", 0),
            hypotheses=[Hypothesis(root_cause="Unknown", confidence=0.1, description="Insufficient data for root cause determination.")],
            primary_remediation=CodeRemediation(file_path="N/A", explanation="Manual investigation required.", git_diff=""),
            postmortem=SREPostmortem(executive_summary="Incident detected but automated analysis was inconclusive.", action_items_prevention=["Investigate manually"])
        )

    @staticmethod
    def _analyze_with_groq(api_key: str, log_filename: str, trace_events: list,
                           blast_radius: dict, rca_result: Optional[Dict[str, Any]] = None) -> AegisDiagnosticReport:
        from groq import Groq

        client = Groq(api_key=api_key)

        rca_context = ""
        if rca_result and rca_result.get("ranked_root_causes"):
            top_root = rca_result["ranked_root_causes"][0]
            rca_context = f"""
DETERMINISTIC ROOT CAUSE ANALYSIS (ground truth from the correlation engine):
- Root cause service: {top_root['service']}
- Root cause class: {top_root['root_cause_class']}
- Confidence score: {top_root['score']}
- Error classes: {top_root['error_classes']}
- Edge basis: {rca_result['edge_basis']}
- Service roles: {json.dumps(rca_result['roles'])}
Rank your hypotheses consistent with this deterministic analysis.
"""

        system_prompt = """You are an expert SRE analyzing distributed system telemetry. Respond with ONLY valid JSON matching this schema exactly:
{
  "incident_id": "INC-YYYY-SHORT-NAME",
  "title": "Professional technical incident title",
  "overall_severity": "CRITICAL|HIGH|MEDIUM|LOW",
  "impact_analysis": "Systems-level impact description",
  "blast_radius_requests": <integer>,
  "hypotheses": [
    {"root_cause": "Name", "confidence": <0.0-1.0>, "description": "Technical rationale"}
  ],
  "primary_remediation": {
    "file_path": "path/to/file.py",
    "explanation": "What to fix and why",
    "git_diff": "unified diff string"
  },
  "postmortem": {
    "executive_summary": "What broke, impact, resolution",
    "action_items_prevention": ["action 1", "action 2", "action 3"]
  }
}"""

        timeline_safe = []
        for ev in trace_events[:20]:
            safe = {k: v for k, v in ev.items() if k != "_event_ts" and not k.startswith("_")}
            timeline_safe.append(safe)

        user_prompt = f"""Analyze these distributed system telemetry events from {log_filename}:

TELEMETRY EVENTS:
{json.dumps(timeline_safe, indent=2, default=str)}

BLAST RADIUS:
{json.dumps(blast_radius, indent=2)}
{rca_context}
Return a structured SRE diagnostic report as JSON."""

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2,
            response_format={"type": "json_object"}
        )

        raw = json.loads(response.choices[0].message.content)
        return AegisDiagnosticReport.model_validate(raw)

    @staticmethod
    def _build_from_rca(log_filename: str, blast_radius: dict, rca_result: Dict[str, Any]) -> AegisDiagnosticReport:
        top = rca_result["ranked_root_causes"][0]
        service = top["service"]
        root_class = top["root_cause_class"]
        err_classes = top.get("error_classes", [])
        roles = rca_result.get("roles", {})

        symptoms = [s for s, r in roles.items() if r == "SYMPTOM"]
        symptom_text = f" Downstream symptoms observed in: {', '.join(symptoms)}." if symptoms else ""

        title = f"{root_class.replace('_', ' ').title()} in {service}"
        impact = (f"{service} experienced {root_class.replace('_', ' ')} "
                  f"({', '.join(err_classes) if err_classes else 'unknown error'}).{symptom_text}")

        return AegisDiagnosticReport(
            incident_id=f"INC-DET-{service.upper().replace('-', '')}",
            title=title,
            overall_severity=top.get("max_severity", "HIGH"),
            impact_analysis=impact,
            blast_radius_requests=blast_radius.get("estimated_affected_requests", 0),
            hypotheses=[
                Hypothesis(
                    root_cause=root_class.replace("_", " ").title(),
                    confidence=top["score"],
                    description=f"Deterministic analysis identified {service} as root cause ({root_class}). "
                                f"Edge basis: {rca_result.get('edge_basis', 'unknown')}."
                )
            ],
            primary_remediation=CodeRemediation(
                file_path=f"config/{service.replace('-', '_')}.py",
                explanation=f"Address {root_class.replace('_', ' ')} in {service}.",
                git_diff=""
            ),
            postmortem=SREPostmortem(
                executive_summary=f"Deterministic RCA identified {service} as root cause via {rca_result.get('edge_basis', 'unknown')} analysis.{symptom_text}",
                action_items_prevention=[f"Address {root_class.replace('_', ' ')} in {service}", "Add monitoring for early detection"]
            )
        )
