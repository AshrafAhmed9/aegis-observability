from correlator import Correlator, IDLE_GAP_SECONDS, MAX_TRACE_SECONDS


def event(trace_id, timestamp, service="api"):
    return {"trace_id": trace_id, "span_id": f"span-{timestamp}", "parent_span_id": None,
            "service": service, "timestamp": timestamp, "severity": "INFO", "error_class": None}


def test_add_event_creates_open_trace():
    correlator = Correlator()
    correlator.add_event(event("t1", 0.0))
    assert "t1" in correlator.open_traces


def test_watermark_only_moves_forward():
    correlator = Correlator()
    correlator.add_event(event("t1", 100.0))
    watermark_after_first = correlator.watermark
    correlator.add_event(event("t1", 50.0))  # an out-of-order, earlier event
    assert correlator.watermark == watermark_after_first


def test_late_event_after_seal_is_dropped():
    correlator = Correlator()
    correlator.add_event(event("t1", 0.0))
    correlator.flush_all()
    correlator.add_event(event("t1", 1.0))
    assert "t1" not in correlator.open_traces


def test_idle_gap_closes_a_quiet_trace():
    correlator = Correlator()
    correlator.add_event(event("t1", 0.0))
    correlator.add_event(event("t2", IDLE_GAP_SECONDS + 20.0))  # pushes the watermark far ahead
    sealed = correlator.close_finished_traces()
    sealed_ids = {e[0]["trace_id"] for e in sealed}
    assert "t1" in sealed_ids


def test_idle_gap_does_not_close_too_early():
    correlator = Correlator()
    correlator.add_event(event("t1", 0.0))
    correlator.add_event(event("t2", 1.0))  # barely moves the watermark
    sealed = correlator.close_finished_traces()
    assert sealed == []


def test_max_trace_seconds_force_closes_long_trace():
    correlator = Correlator()
    correlator.add_event(event("t1", 0.0))
    correlator.add_event(event("t1", MAX_TRACE_SECONDS + 5.0))
    sealed = correlator.close_finished_traces()
    sealed_ids = {e[0]["trace_id"] for e in sealed}
    assert "t1" in sealed_ids


def test_sealed_trace_is_sorted_by_timestamp():
    correlator = Correlator()
    correlator.add_event(event("t1", 5.0))
    correlator.add_event(event("t1", 1.0))
    correlator.add_event(event("t1", 3.0))
    sealed = correlator.flush_all()[0]
    timestamps = [e["timestamp"] for e in sealed]
    assert timestamps == sorted(timestamps)


def test_flush_all_closes_everything():
    correlator = Correlator()
    correlator.add_event(event("t1", 0.0))
    correlator.add_event(event("t2", 0.0))
    sealed = correlator.flush_all()
    assert len(sealed) == 2
    assert correlator.open_traces == {}


def test_independent_traces_are_buffered_separately():
    correlator = Correlator()
    correlator.add_event(event("t1", 0.0))
    correlator.add_event(event("t2", 0.0))
    assert len(correlator.open_traces["t1"].events) == 1
    assert len(correlator.open_traces["t2"].events) == 1


def test_flush_all_on_empty_correlator_returns_empty_list():
    correlator = Correlator()
    assert correlator.flush_all() == []


def test_seal_removes_trace_from_open_traces():
    correlator = Correlator()
    correlator.add_event(event("t1", 0.0))
    correlator.flush_all()
    assert "t1" not in correlator.open_traces
    assert "t1" in correlator.closed_trace_ids
