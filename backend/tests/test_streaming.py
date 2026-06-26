from app.streaming import parse_event_time, dedupe_key, StreamingCorrelator, EmittedTrace


def test_parse_event_time_basic():
    ev = {"timestamp": "2026-05-28T10:41:00.012Z"}
    t = parse_event_time(ev)
    assert isinstance(t, float)
    t2 = parse_event_time({"timestamp": "2026-05-28T10:41:00.034Z"})
    assert t2 > t


def test_parse_event_time_missing_or_bad():
    assert parse_event_time({}) is None
    assert parse_event_time({"timestamp": ""}) is None
    assert parse_event_time({"timestamp": "not-a-date"}) is None


def test_dedupe_key_same_event_is_equal():
    ev = {"trace_id": "tr-1", "span_id": "sp-1", "timestamp": "2026-05-28T10:41:00.012Z"}
    assert dedupe_key(ev) == dedupe_key(dict(ev))


def test_dedupe_key_same_span_different_time_is_distinct():
    a = {"trace_id": "tr-1", "span_id": "sp-209", "timestamp": "2026-05-28T16:20:05.445Z"}
    b = {"trace_id": "tr-1", "span_id": "sp-209", "timestamp": "2026-05-28T16:20:06.000Z"}
    assert dedupe_key(a) != dedupe_key(b)


def test_dedupe_key_fallback_without_span():
    ev = {"trace_id": "tr-1", "raw_log": "some line", "timestamp": "2026-05-28T10:41:00.012Z"}
    assert dedupe_key(ev) == dedupe_key(dict(ev))


def _make_event(trace_id, span_id, ts, **extra):
    ev = {"trace_id": trace_id, "span_id": span_id, "timestamp": ts}
    ev.update(extra)
    return ev


def test_correlator_basic_flush():
    c = StreamingCorrelator()
    c.ingest(_make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z"))
    c.ingest(_make_event("tr-1", "sp-2", "2026-05-28T10:41:01.000Z"))
    c.ingest(_make_event("tr-2", "sp-3", "2026-05-28T10:41:00.500Z"))
    result = c.flush_all()
    trace_ids = {r.trace_id for r in result}
    assert trace_ids == {"tr-1", "tr-2"}
    tr1 = [r for r in result if r.trace_id == "tr-1"][0]
    assert len(tr1.events) == 2


def test_correlator_idle_gap_closes_trace():
    c = StreamingCorrelator(idle_gap=5.0, grace=2.0)
    c.ingest(_make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z"))
    closed = c.ingest(_make_event("tr-2", "sp-2", "2026-05-28T10:41:08.000Z"))
    closed_ids = {r.trace_id for r in closed}
    assert "tr-1" in closed_ids


def test_correlator_dedupe():
    c = StreamingCorrelator()
    ev = _make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z")
    c.ingest(ev)
    c.ingest(dict(ev))
    result = c.flush_all()
    assert len(result[0].events) == 1


def test_correlator_late_event():
    c = StreamingCorrelator(idle_gap=5.0, grace=2.0)
    c.ingest(_make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z"))
    c.ingest(_make_event("tr-2", "sp-2", "2026-05-28T10:41:08.000Z"))
    c.ingest(_make_event("tr-1", "sp-3", "2026-05-28T10:41:01.000Z"))
    assert c.late_count == 1
    assert c.open_trace_count == 1


def test_correlator_max_window():
    c = StreamingCorrelator(window_max=10.0, idle_gap=100.0, grace=0.0)
    c.ingest(_make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z"))
    # Second event spans 11s — exceeds max_window, so ingest itself closes it
    closed = c.ingest(_make_event("tr-1", "sp-2", "2026-05-28T10:41:11.000Z"))
    assert len(closed) == 1
    assert closed[0].trace_id == "tr-1"


def test_correlator_lru_eviction():
    c = StreamingCorrelator(max_open=2)
    c.ingest(_make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z"))
    c.ingest(_make_event("tr-2", "sp-2", "2026-05-28T10:41:01.000Z"))
    evicted = c.ingest(_make_event("tr-3", "sp-3", "2026-05-28T10:41:02.000Z"))
    evicted_ids = {r.trace_id for r in evicted}
    assert "tr-1" in evicted_ids
    assert c.open_trace_count == 2


def test_correlator_deterministic_sort():
    c = StreamingCorrelator()
    c.ingest(_make_event("tr-1", "sp-2", "2026-05-28T10:41:00.000Z"))
    c.ingest(_make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z"))
    c.ingest(_make_event("tr-1", "sp-3", "2026-05-28T10:40:59.000Z"))
    result = c.flush_all()
    spans = [e["span_id"] for e in result[0].events]
    assert spans == ["sp-3", "sp-1", "sp-2"]


def test_correlator_no_trace_id_ignored():
    c = StreamingCorrelator()
    result = c.ingest({"span_id": "sp-1", "timestamp": "2026-05-28T10:41:00.000Z"})
    assert result == []
    assert c.open_trace_count == 0
from app.streaming import IncidentAssembler

def test_assembler_single_bucket_flush():
    """All traces in one window → flush returns combined events."""
    a = IncidentAssembler(window=120.0)
    t1 = EmittedTrace("tr-1", [_make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z")],
                       min_ts=1748429260.0, max_ts=1748429260.0)
    t2 = EmittedTrace("tr-2", [_make_event("tr-2", "sp-2", "2026-05-28T10:41:30.000Z")],
                       min_ts=1748429290.0, max_ts=1748429290.0)
    assert a.add(t1) is None
    assert a.add(t2) is None
    result = a.flush()
    assert len(result) == 2
    assert result[0]["span_id"] == "sp-1"  # earlier event first
    assert result[1]["span_id"] == "sp-2"


def test_assembler_window_rollover():
    """Trace arriving past window boundary triggers finalize of previous bucket."""
    a = IncidentAssembler(window=60.0)
    t1 = EmittedTrace("tr-1", [_make_event("tr-1", "sp-1", "2026-05-28T10:41:00.000Z")],
                       min_ts=1748429260.0, max_ts=1748429260.0)
    assert a.add(t1) is None
    # 90 seconds later → past the 60s window
    t2 = EmittedTrace("tr-2", [_make_event("tr-2", "sp-2", "2026-05-28T10:42:30.000Z")],
                       min_ts=1748429350.0, max_ts=1748429350.0)
    result = a.add(t2)
    assert result is not None
    assert len(result) == 1  # only t1's events; t2 is in the new bucket
    assert result[0]["span_id"] == "sp-1"
    # t2 should be in the new bucket
    remaining = a.flush()
    assert len(remaining) == 1
    assert remaining[0]["span_id"] == "sp-2"


def test_assembler_empty_flush():
    """Flush on empty assembler returns None."""
    a = IncidentAssembler()
    assert a.flush() is None
