from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import os
import json
from groq import Groq

# =====================================================================
# Aegis Pydantic Schemas (Structured Output Definitions)
# =====================================================================

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
    action_items_prevention: List[str] = Field(description="Steps to prevent recurrence (e.g. alerts, config)")

class AegisDiagnosticReport(BaseModel):
    incident_id: str = Field(description="SRE Incident Code (e.g. INC-2026-REDIS)")
    title: str = Field(description="Professional, technical incident title")
    overall_severity: str = Field(description="CRITICAL, HIGH, MEDIUM, LOW")
    impact_analysis: str = Field(description="Systems-level business and database impact summary")
    blast_radius_requests: int = Field(description="Estimated number of affected requests")
    hypotheses: List[Hypothesis] = Field(description="Ranked possible root causes with confidence scores")
    primary_remediation: CodeRemediation = Field(description="Specific actionable code fix")
    postmortem: SREPostmortem = Field(description="Standardized postmortem analysis")

# =====================================================================
# High-Fidelity Pre-Packaged SRE Diagnostics (Fallback / Offline Engine)
# =====================================================================

OFFLINE_DIAGNOSTICS = {
    "redis_retry_storm.log": AegisDiagnosticReport(
        incident_id="INC-2026-REDIS-STORM",
        title="Redis Connection Pool Exhaustion & Celery Worker Thread Starvation Cascade",
        overall_severity="CRITICAL",
        impact_analysis="Connection starvation in the Redis caching layer blocked asynchronous payment processing. Unhandled worker timeouts triggered a cascade retry storm, leading to CPU exhaustion, worker crash loops, and a 100% gateway checkout timeout rate for all active customer cart transactions.",
        blast_radius_requests=13050,
        hypotheses=[
            Hypothesis(
                root_cause="Redis Connection Pool Capacity Starvation",
                confidence=0.95,
                description="The local Redis instance max active connection ceiling (200) was breached. Downstream client processes stalled in blocking wait queues, eventually timing out after 5000ms."
            ),
            Hypothesis(
                root_cause="Exponential Backoff Misconfiguration / Task Retry Storm",
                confidence=0.88,
                description="Celery worker payment tasks failed to employ jittered exponential backoff. Retries occurred in tight sync intervals, creating a thundering herd retry storm that overwhelmed worker thread pools."
            )
        ],
        primary_remediation=CodeRemediation(
            file_path="infra/config/redis_pool.py",
            explanation="Reconfigure the Redis connection pool with a dynamic worker-bounded max capacity and non-blocking acquisition timeouts. Add a jittered backoff factor to Celery task retries to decouple concurrent processing requests.",
            git_diff="""diff --git a/infra/config/redis_pool.py b/infra/config/redis_pool.py
index a54f9d2..8bc32d1 100644
--- a/infra/config/redis_pool.py
+++ b/infra/config/redis_pool.py
@@ -10,6 +10,12 @@
-redis_pool = redis.ConnectionPool(host='localhost', port=6379, max_connections=200)
+# Upgraded pool connection limits and added non-blocking acquisition timeouts
+redis_pool = redis.ConnectionPool(
+    host='localhost', 
+    port=6379, 
+    max_connections=1000, 
+    timeout=2.0, 
+    socket_timeout=5.0
+)
 
 def get_redis_client():
-    return redis.Redis(connection_pool=redis_pool)
+    # Return connection from enlarged pool with retry strategies enabled
+    return redis.Redis(connection_pool=redis_pool, retry_on_timeout=True)
"""
        ),
        postmortem=SREPostmortem(
            executive_summary="On 2026-05-28, the checkout service encountered a total outage due to a Redis connection pool starvation. Celery worker tasks entered tight retry loops, escalating the pool load until thread pool starvation crashed the payment workers.",
            action_items_prevention=[
                "Configure Prometheus alerts for Redis connection pool utilization exceeding 80%.",
                "Refactor all async tasks to enforce jittered exponential backoff retries.",
                "Enforce circuit breakers on critical cache operations to allow graceful degradation."
            ]
        )
    ),
    "pg_deadlock.log": AegisDiagnosticReport(
        incident_id="INC-2026-PG-DEADLOCK",
        title="PostgreSQL Row Lock Deadlock during Parallel Checkout Transactions",
        overall_severity="HIGH",
        impact_analysis="Concurrent checkout transactions TX_001 and TX_002 attempted row-update locking on identical inventory SKU rows (9982 and 7711) in reversed execution order. This triggered a database transaction deadlock, resulting in transaction rollbacks, SQLAlchemy OperationalErrors, and gateway checkouts failing with HTTP 500 errors.",
        blast_radius_requests=4350,
        hypotheses=[
            Hypothesis(
                root_cause="Non-Deterministic Database Lock Acquisition Order",
                confidence=0.98,
                description="Checkout transactions acquired database locks in non-deterministic ordering (TX_001 locked 9982 then blocked on 7711, while TX_002 locked 7711 then blocked on 9982), creating a circular wait dependency."
            ),
            Hypothesis(
                root_cause="Transaction Isolation Level Lock Contention",
                confidence=0.74,
                description="High concurrency checkout requests executing under SERIALIZABLE or high lock isolation durations held record-level locks long enough to trigger race deadlocks under load."
            )
        ],
        primary_remediation=CodeRemediation(
            file_path="backend/services/inventory.py",
            explanation="Sort inventory update locks consistently by record ID before acquiring row-exclusive locks (`SELECT FOR UPDATE`). This guarantees lock acquisition ordering is always deterministic, completely eliminating lock deadlock conditions.",
            git_diff="""diff --git a/backend/services/inventory.py b/backend/services/inventory.py
index cb981d0..4af29df 100644
--- a/backend/services/inventory.py
+++ b/backend/services/inventory.py
@@ -14,5 +14,8 @@
 def lock_checkout_items(session, item_ids):
+    # CRITICAL: Sort item_ids deterministically to prevent Postgres transaction deadlocks
+    sorted_item_ids = sorted(list(set(item_ids)))
-    for item_id in item_ids:
+    for item_id in sorted_item_ids:
         session.query(Inventory).filter_by(id=item_id).with_for_update().first()
"""
        ),
        postmortem=SREPostmortem(
            executive_summary="Concurrent checkout requests for identical inventory items locked database records in reversed order, causing PostgreSQL to terminate the transactions due to a detected deadlock.",
            action_items_prevention=[
                "Implement strict, deterministic sorting on all database row lock requests.",
                "Reduce transaction scope to the absolute minimum necessary steps.",
                "Add database deadlock automatic retry handling at the application repository layer."
            ]
        )
    ),
    "cache_stampede.log": AegisDiagnosticReport(
        incident_id="INC-2026-CACHE-STAMPEDE",
        title="Cache Stampede & PostgreSQL Connection Pool Starvation under Spike Load",
        overall_severity="CRITICAL",
        impact_analysis="A sudden rate-limiter sync failure allowed a massive concurrent request spike to bypass throttles. The popular products cache key expired simultaneously, causing a cache miss stampede where hundreds of worker threads queried the primary database concurrently, exhausting Postgres connection pools.",
        blast_radius_requests=18340,
        hypotheses=[
            Hypothesis(
                root_cause="Cache Miss Stampede (Cache Thundering Herd)",
                confidence=0.96,
                description="The popular products cache key expired, causing all concurrent API requests to miss the cache simultaneously and dispatch expensive database query joins, flooding the connection pool."
            ),
            Hypothesis(
                root_cause="Rate Limiter Synchronization Latency",
                confidence=0.82,
                description="Limiter sync latency triggered local instance fallback bypasses, allowing an un-throttled spike of concurrent queries to hit backend services."
            )
        ],
        primary_remediation=CodeRemediation(
            file_path="backend/services/product.py",
            explanation="Implement a single-flight mutex pattern (or lock locking) to ensure only a single worker thread queries the database to re-populate the cache on expiration, while other concurrent reads wait or read slightly stale data.",
            git_diff="""diff --git a/backend/services/product.py b/backend/services/product.py
index dc84198..ab84210 100644
--- a/backend/services/product.py
+++ b/backend/services/product.py
@@ -18,8 +18,17 @@
 def get_popular_products():
     products = cache.get('prod:popular')
     if not products:
-        products = db.query("SELECT * FROM products JOIN reviews...")
-        cache.set('prod:popular', products, ttl=3600)
+        # Single-flight locking pattern to prevent Cache Stampedes
+        with cache.lock('lock:prod:popular', timeout=10):
+            # Double-check inside the lock to see if another thread populated it
+            products = cache.get('prod:popular')
+            if not products:
+                products = db.query("SELECT * FROM products JOIN reviews...")
+                cache.set('prod:popular', products, ttl=3600)
     return products
"""
        ),
        postmortem=SREPostmortem(
            executive_summary="Cache key expiration under un-throttled request spikes led to database connection pool starvation. Upstream checkout gateways failed with Service Unavailable errors.",
            action_items_prevention=[
                "Implement a single-flight lock mechanism for high-demand cache key re-population.",
                "Introduce jitter to cache TTL values to prevent simultaneous multi-key invalidation.",
                "Upgrade the rate-limiter sync heartbeat frequency to reduce local bypass windows."
            ]
        )
    )
}

# =====================================================================
# Aegis AI Diagnostic Engine Runner
# =====================================================================

class AegisAnalyzer:
    """
    Aegis AI Diagnostic Engine.
    Exposes direct interfaces for live AI analysis and offline SRE-grade diagnostics.
    """
    
    @staticmethod
    def _analyze_with_groq(api_key: str, log_filename: str, timeline_events: List[Dict[str, Any]], blast_radius: Dict[str, Any]) -> AegisDiagnosticReport:
        client = Groq(api_key=api_key)

        system_prompt = """You are an expert SRE analyzing distributed system telemetry. Respond with ONLY valid JSON — no markdown, no explanation — matching this schema exactly:
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

        user_prompt = f"""Analyze these distributed system telemetry events from {log_filename}:

TELEMETRY EVENTS:
{json.dumps(timeline_events[:20], indent=2)}

BLAST RADIUS:
{json.dumps(blast_radius, indent=2)}

Return a structured SRE diagnostic report as JSON."""

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.2
        )

        raw = json.loads(response.choices[0].message.content)
        return AegisDiagnosticReport(
            incident_id=raw["incident_id"],
            title=raw["title"],
            overall_severity=raw["overall_severity"],
            impact_analysis=raw["impact_analysis"],
            blast_radius_requests=raw.get("blast_radius_requests", blast_radius.get("estimated_affected_requests", 0)),
            hypotheses=[Hypothesis(**h) for h in raw["hypotheses"]],
            primary_remediation=CodeRemediation(**raw["primary_remediation"]),
            postmortem=SREPostmortem(**raw["postmortem"])
        )

    @staticmethod
    def analyze(log_filename: str, timeline_events: List[Dict[str, Any]], blast_radius: Dict[str, Any]) -> AegisDiagnosticReport:
        """
        Analyzes the deterministic timeline and produces the diagnostic report.
        Tries Groq AI first if GROQ_API_KEY is set, then falls back to offline diagnostics.
        """
        # Try live AI analysis via Groq if API key is configured
        api_key = os.environ.get("GROQ_API_KEY")
        if api_key:
            try:
                print(f"[Aegis] Running live Groq AI analysis for {log_filename}...")
                return AegisAnalyzer._analyze_with_groq(api_key, log_filename, timeline_events, blast_radius)
            except Exception as e:
                print(f"[Aegis] Groq API failed ({e}), falling back to offline diagnostics.")

        # Determine the key to match
        filename_key = os.path.basename(log_filename)

        if filename_key in OFFLINE_DIAGNOSTICS:
            report = OFFLINE_DIAGNOSTICS[filename_key]
            report.blast_radius_requests = blast_radius.get("estimated_affected_requests", report.blast_radius_requests)
            return report
            
        # Default report for custom uploaded logs if not matched in pre-packaged scenarios
        return AegisDiagnosticReport(
            incident_id=f"INC-{os.urandom(3).hex().upper()}",
            title=f"Anomaly and Failure Incident detected in {log_filename}",
            overall_severity="HIGH",
            impact_analysis="Uncorrelated trace events indicate transaction bottlenecks or downstream connectivity degradation.",
            blast_radius_requests=blast_radius.get("estimated_affected_requests", 1200),
            hypotheses=[
                Hypothesis(
                    root_cause="Generic Telemetry Latency Bottleneck",
                    confidence=0.75,
                    description="Telemetry logs contain 5xx response flags or transaction locks. Downstream processes waiting on threads or external APIs."
                )
            ],
            primary_remediation=CodeRemediation(
                file_path="unknown_file.py",
                explanation="Examine raw application traces to locate database lock contentions or long-running CPU calculations.",
                git_diff="""diff --git a/unknown_file.py b/unknown_file.py
--- a/unknown_file.py
+++ b/unknown_file.py
@@ -1 +1,2 @@
+# Monitor CPU pool usage and network latency
 """
            ),
            postmortem=SREPostmortem(
                executive_summary="Unstructured telemetry log execution bottleneck detected. Investigation required.",
                action_items_prevention=[
                    "Add comprehensive distributed trace logging across all systems.",
                    "Establish performance alerts for API latency exceeding 2000ms."
                ]
            )
        )
