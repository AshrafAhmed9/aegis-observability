from ml.features import window_features, candidate_features, incident_signature


def _ev(ts, **fields):
    e = {"_event_ts": ts, "severity": "INFO"}
    e.update(fields)
    return e


def test_window_features_none_when_sparse():
    events = [_ev(0.0, connection_pool_usage=0.5)]
    assert window_features(events, window_end=10.0) is None


def test_window_features_deterministic():
    events = [_ev(i, connection_pool_usage=0.4 + i * 0.01) for i in range(10)]
    f1 = window_features(events, window_end=10.0, window_seconds=60.0)
    f2 = window_features(events, window_end=10.0, window_seconds=60.0)
    assert f1 == f2
    assert f1["connection_pool_usage__slope"] > 0
    assert f1["event_count"] == 10


def test_window_features_error_counts():
    events = [_ev(i, severity="ERROR" if i % 2 == 0 else "INFO") for i in range(6)]
    f = window_features(events, window_end=6.0, window_seconds=60.0)
    assert f["error_count"] == 3
    assert f["event_count"] == 6


def test_candidate_features_severity_and_error_class():
    nodes = [
        {"service_name": "redis-cache", "max_severity": "CRITICAL",
         "error_classes": ["redis.exceptions.ConnectionError"],
         "first_error_ts": "t1", "latency_ms": 10.0, "event_count": 5},
        {"service_name": "api-gateway", "max_severity": "ERROR", "error_classes": ["GatewayTimeout"],
         "first_error_ts": "t2", "latency_ms": 100.0, "event_count": 5},
    ]
    edges = [{"source_service": "api-gateway", "target_service": "redis-cache", "propagation_type": "RPC"}]
    feats = candidate_features(nodes[0], nodes, edges, topo_order=["redis-cache", "api-gateway"])
    assert feats["severity_rank"] == 1.0
    assert feats["has_connection_class"] == 1.0
    assert feats["has_deadlock_class"] == 0.0


def test_incident_signature_none_when_healthy():
    nodes = [{"service_name": "api-gateway", "max_severity": "INFO", "error_classes": [],
              "first_error_ts": None, "latency_ms": 10.0, "event_count": 3}]
    rca_result = {"ranked_root_causes": []}
    assert incident_signature(nodes, rca_result, []) is None


def test_incident_signature_includes_fault_class_and_services():
    nodes = [{"service_name": "postgres-db", "max_severity": "CRITICAL",
              "error_classes": ["postgres.deadlock"], "first_error_ts": "t1",
              "latency_ms": 10.0, "event_count": 3}]
    rca_result = {"ranked_root_causes": [{"service": "postgres-db", "root_cause_class": "deadlock"}]}
    events = [{"severity": "CRITICAL", "msg": "deadlock detected"}]
    sig = incident_signature(nodes, rca_result, events)
    assert sig["fault_class"] == "deadlock"
    assert sig["root_cause_service"] == "postgres-db"
    assert "postgres-db" in sig["signature"]
