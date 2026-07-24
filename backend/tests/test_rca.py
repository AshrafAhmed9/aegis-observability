import rca
from correlator import Correlator
from simulator import generate_episodes


def event(service, timestamp, severity="INFO", span_id=None, parent_span_id=None, error_class=None):
    return {"trace_id": "t1", "span_id": span_id or f"{service}-{timestamp}", "parent_span_id": parent_span_id,
            "service": service, "timestamp": timestamp, "severity": severity, "error_class": error_class}


def test_build_graph_tracks_max_severity_per_service():
    events = [event("api", 0, "INFO"), event("api", 1, "ERROR")]
    nodes, _ = rca.build_graph(events)
    assert nodes["api"]["max_severity"] == "ERROR"


def test_build_graph_draws_edge_from_parent_span_service():
    events = [
        event("cache", 0, "ERROR", span_id="cache-span"),
        event("api", 1, "CRITICAL", parent_span_id="cache-span"),
    ]
    _, edges = rca.build_graph(events)
    assert ("cache", "api") in edges


def test_build_graph_no_self_edge_same_service():
    events = [
        event("api", 0, "INFO", span_id="a"),
        event("api", 1, "INFO", parent_span_id="a"),
    ]
    _, edges = rca.build_graph(events)
    assert edges == set()


def test_rank_root_causes_single_degraded_service():
    nodes = {"api": {"max_severity": "ERROR", "error_classes": set(), "first_error_time": 0}}
    assert rca.rank_root_causes(nodes, set()) == ["api"]


def test_rank_root_causes_no_degraded_services():
    nodes = {"api": {"max_severity": "INFO", "error_classes": set(), "first_error_time": None}}
    assert rca.rank_root_causes(nodes, set()) == []


def test_rank_root_causes_orders_cause_before_effect():
    nodes = {
        "cache": {"max_severity": "ERROR", "error_classes": set(), "first_error_time": 0},
        "api": {"max_severity": "CRITICAL", "error_classes": set(), "first_error_time": 5},
    }
    edges = {("cache", "api")}
    ranked = rca.rank_root_causes(nodes, edges)
    assert ranked[0] == "cache"


def test_rank_root_causes_falls_back_to_earliest_error_without_edges():
    nodes = {
        "cache": {"max_severity": "ERROR", "error_classes": set(), "first_error_time": 10},
        "queue": {"max_severity": "ERROR", "error_classes": set(), "first_error_time": 2},
    }
    ranked = rca.rank_root_causes(nodes, set())
    assert ranked[0] == "queue"


def test_rank_root_causes_handles_a_two_node_cycle():
    nodes = {
        "a": {"max_severity": "ERROR", "error_classes": set(), "first_error_time": 0},
        "b": {"max_severity": "ERROR", "error_classes": set(), "first_error_time": 1},
    }
    edges = {("a", "b"), ("b", "a")}
    ranked = rca.rank_root_causes(nodes, edges)
    assert set(ranked) == {"a", "b"}


def test_classify_error_type_deadlock():
    assert rca.classify_error_type({"postgres.deadlock"}) == "deadlock"


def test_classify_error_type_resource_exhaustion():
    assert rca.classify_error_type({"redis.exceptions.ConnectionError"}) == "resource_exhaustion"


def test_classify_error_type_unknown_defaults_to_failure():
    assert rca.classify_error_type({"some.other.Error"}) == "failure"


def _run_through_pipeline(events):
    correlator = Correlator()
    for e in events:
        correlator.add_event(e)
    sealed = correlator.flush_all()[0]
    return rca.analyze(sealed)


def test_analyze_end_to_end_identifies_redis_leak_root_cause():
    from simulator import generate_episode
    events, root_cause_service, _, _ = generate_episode(seed=1, fault_name="redis_leak")
    result = _run_through_pipeline(events)
    assert result["ranked_root_causes"][0] == root_cause_service


def test_all_78_generated_episodes_are_correctly_diagnosed():
    episodes = generate_episodes()
    assert len(episodes) == 78
    for episode in episodes:
        result = _run_through_pipeline(episode["events"])
        assert result["ranked_root_causes"][0] == episode["root_cause_service"]
