from app.correlation import CorrelationEngine


def test_redis_storm_root_cause():
    """redis-cache should be ROOT_CAUSE, api-gateway should be SYMPTOM."""
    nodes = [
        {"service_name": "api-gateway", "max_severity": "CRITICAL", "error_classes": ["GatewayTimeout"], "latency_ms": 15004.8, "event_count": 3},
        {"service_name": "checkout-service", "max_severity": "INFO", "error_classes": [], "latency_ms": 0, "event_count": 2},
        {"service_name": "redis-cache", "max_severity": "ERROR", "error_classes": ["redis.exceptions.ConnectionError"], "latency_ms": 5000, "event_count": 4},
        {"service_name": "celery-worker", "max_severity": "CRITICAL", "error_classes": [], "latency_ms": 0, "event_count": 5},
        {"service_name": "postgres-db", "max_severity": "INFO", "error_classes": [], "latency_ms": 120.4, "event_count": 2},
    ]
    edges = [
        {"source_service": "api-gateway", "target_service": "checkout-service", "propagation_type": "RPC"},
        {"source_service": "checkout-service", "target_service": "postgres-db", "propagation_type": "RPC"},
        {"source_service": "checkout-service", "target_service": "celery-worker", "propagation_type": "RPC"},
        {"source_service": "celery-worker", "target_service": "redis-cache", "propagation_type": "RPC"},
    ]
    result = CorrelationEngine.classify_root_cause(nodes, edges)
    assert result["roles"]["redis-cache"] == "ROOT_CAUSE"
    assert result["roles"]["api-gateway"] == "SYMPTOM"
    assert result["roles"]["celery-worker"] == "SYMPTOM"
    assert result["ranked_root_causes"][0]["service"] == "redis-cache"
    assert result["ranked_root_causes"][0]["root_cause_class"] == "resource_exhaustion"


def test_pg_deadlock_root_cause():
    """postgres-db with deadlock err_class should be CYCLE_MEMBER root."""
    nodes = [
        {"service_name": "api-gateway", "max_severity": "CRITICAL", "error_classes": ["InternalServerError"], "latency_ms": 100, "event_count": 2},
        {"service_name": "order-service", "max_severity": "ERROR", "error_classes": ["sqlalchemy.exc.OperationalError"], "latency_ms": 50, "event_count": 3},
        {"service_name": "postgres-db", "max_severity": "ERROR", "error_classes": ["postgres.deadlock"], "latency_ms": 3330, "event_count": 5},
    ]
    edges = [
        {"source_service": "api-gateway", "target_service": "order-service", "propagation_type": "RPC"},
        {"source_service": "order-service", "target_service": "postgres-db", "propagation_type": "RPC"},
    ]
    result = CorrelationEngine.classify_root_cause(nodes, edges)
    assert result["roles"]["postgres-db"] == "CYCLE_MEMBER"
    assert result["roles"]["api-gateway"] == "SYMPTOM"
    assert result["cycle"]["detected"] is True
    assert result["cycle"]["kind"] == "deadlock"
    assert result["ranked_root_causes"][0]["service"] == "postgres-db"


def test_no_degraded_services():
    """All healthy → empty roles, no root causes."""
    nodes = [
        {"service_name": "api-gateway", "max_severity": "INFO", "error_classes": [], "latency_ms": 45, "event_count": 1},
        {"service_name": "checkout-service", "max_severity": "INFO", "error_classes": [], "latency_ms": 10, "event_count": 1},
    ]
    edges = [{"source_service": "api-gateway", "target_service": "checkout-service", "propagation_type": "RPC"}]
    result = CorrelationEngine.classify_root_cause(nodes, edges)
    assert result["roles"] == {}
    assert result["ranked_root_causes"] == []


def test_temporal_edges_lower_confidence():
    """TEMPORAL edge basis should multiply scores by 0.7."""
    nodes = [
        {"service_name": "redis-cache", "max_severity": "ERROR", "error_classes": ["redis.exceptions.ConnectionError"], "latency_ms": 5000, "event_count": 1},
        {"service_name": "api-gateway", "max_severity": "CRITICAL", "error_classes": ["GatewayTimeout"], "latency_ms": 15000, "event_count": 1},
    ]
    edges = [
        {"source_service": "api-gateway", "target_service": "redis-cache", "propagation_type": "TEMPORAL"},
    ]
    result = CorrelationEngine.classify_root_cause(nodes, edges)
    assert result["edge_basis"] == "TEMPORAL"
    for rc in result["ranked_root_causes"]:
        assert rc["score"] <= 0.7


def test_deterministic_across_runs():
    """Same input → same output, twice."""
    nodes = [
        {"service_name": "redis-cache", "max_severity": "ERROR", "error_classes": ["redis.exceptions.ConnectionError"], "latency_ms": 5000, "event_count": 4},
        {"service_name": "celery-worker", "max_severity": "CRITICAL", "error_classes": [], "latency_ms": 0, "event_count": 5},
        {"service_name": "api-gateway", "max_severity": "CRITICAL", "error_classes": ["GatewayTimeout"], "latency_ms": 15004.8, "event_count": 3},
    ]
    edges = [
        {"source_service": "api-gateway", "target_service": "celery-worker", "propagation_type": "RPC"},
        {"source_service": "celery-worker", "target_service": "redis-cache", "propagation_type": "RPC"},
    ]
    r1 = CorrelationEngine.classify_root_cause(nodes, edges)
    r2 = CorrelationEngine.classify_root_cause(nodes, edges)
    assert r1 == r2
