import simulator


def test_generate_episode_returns_nonempty_events():
    events, _, _, _ = simulator.generate_episode(seed=1, fault_name="redis_leak")
    assert len(events) > 0


def test_redis_leak_root_cause_is_cache():
    _, root_cause_service, _, _ = simulator.generate_episode(seed=1, fault_name="redis_leak")
    assert root_cause_service == "cache"


def test_queue_backlog_root_cause_is_queue():
    _, root_cause_service, _, _ = simulator.generate_episode(seed=1, fault_name="queue_backlog")
    assert root_cause_service == "queue"


def test_deadlock_burst_root_cause_is_database():
    _, root_cause_service, _, _ = simulator.generate_episode(seed=1, fault_name="deadlock_burst")
    assert root_cause_service == "database"


def test_target_service_logs_exactly_one_warning_before_its_error():
    events, root_cause_service, _, _ = simulator.generate_episode(seed=1, fault_name="redis_leak")
    target_events = [e for e in events if e["service"] == root_cause_service]
    warnings = [e for e in target_events if e["severity"] == "WARNING"]
    assert len(warnings) == 1


def test_failure_time_is_after_warning_time():
    _, _, warning_time, failure_time = simulator.generate_episode(seed=1, fault_name="redis_leak")
    assert failure_time > warning_time


def test_cascade_events_are_logged_by_api():
    events, _, _, failure_time = simulator.generate_episode(seed=1, fault_name="redis_leak")
    cascade_events = [e for e in events if e["severity"] == "CRITICAL"]
    assert all(e["service"] == "api" for e in cascade_events)
    assert len(cascade_events) > 0


def test_cascade_events_link_to_targets_error_span():
    events, root_cause_service, _, _ = simulator.generate_episode(seed=1, fault_name="redis_leak")
    target_error = next(e for e in events if e["service"] == root_cause_service and e["severity"] == "ERROR")
    cascade_events = [e for e in events if e["severity"] == "CRITICAL"]
    assert all(e["parent_span_id"] == target_error["span_id"] for e in cascade_events)


def test_baseline_traffic_covers_all_services():
    events, _, _, _ = simulator.generate_episode(seed=1, fault_name="redis_leak")
    baseline_services = {e["service"] for e in events if e["severity"] == "INFO"}
    assert baseline_services == set(simulator.SERVICES)


def test_generate_episodes_default_count_is_78():
    episodes = simulator.generate_episodes()
    assert len(episodes) == 78


def test_generate_episodes_seeds_are_unique():
    episodes = simulator.generate_episodes()
    seeds = [episode["seed"] for episode in episodes]
    assert len(seeds) == len(set(seeds))


def test_same_seed_and_fault_are_reproducible():
    events_a, _, _, _ = simulator.generate_episode(seed=42, fault_name="redis_leak")
    events_b, _, _, _ = simulator.generate_episode(seed=42, fault_name="redis_leak")
    assert events_a == events_b


def test_different_seeds_produce_different_backoff_gaps():
    _, _, w1, f1 = simulator.generate_episode(seed=1, fault_name="redis_leak")
    _, _, w2, f2 = simulator.generate_episode(seed=2, fault_name="redis_leak")
    assert (f1 - w1) != (f2 - w2)


def test_exactly_three_fault_types():
    assert len(simulator.FAULT_TYPES) == 3


def test_span_counter_issues_unique_increasing_ids():
    counter = simulator.SpanCounter()
    ids = [counter.next() for _ in range(3)]
    assert ids == ["span-1", "span-2", "span-3"]


def test_make_event_has_all_required_fields():
    e = simulator.make_event("t1", "s1", None, "api", 0.0, "INFO")
    assert set(e.keys()) == {"trace_id", "span_id", "parent_span_id", "service", "timestamp", "severity", "error_class"}


def test_episodes_per_fault_is_configurable():
    episodes = simulator.generate_episodes(episodes_per_fault=5)
    assert len(episodes) == 15


def test_root_cause_is_never_the_api_service():
    episodes = simulator.generate_episodes(episodes_per_fault=5)
    assert all(episode["root_cause_service"] != "api" for episode in episodes)


def test_all_events_in_an_episode_share_one_trace_id():
    events, _, _, _ = simulator.generate_episode(seed=1, fault_name="redis_leak")
    trace_ids = {e["trace_id"] for e in events}
    assert len(trace_ids) == 1
