from app.infra import parse_aegis_metrics

SAMPLE_EXPOSITION = """\
# HELP python_gc_objects_collected_total Objects collected during gc
# TYPE python_gc_objects_collected_total counter
python_gc_objects_collected_total{generation="0"} 286.0
# HELP aegis_events_ingested_total Total events ingested
# TYPE aegis_events_ingested_total counter
aegis_events_ingested_total 42.0
aegis_events_ingested_created 1.7e+09
# HELP aegis_open_traces Currently open trace buffers
# TYPE aegis_open_traces gauge
aegis_open_traces 3.0
# HELP aegis_predicted_breach_eta_seconds Seconds until predicted breach
# TYPE aegis_predicted_breach_eta_seconds gauge
aegis_predicted_breach_eta_seconds{metric="connection_pool_usage",service="redis-cache"} 97.8
# HELP aegis_correlation_duration_seconds Pipeline duration
# TYPE aegis_correlation_duration_seconds histogram
aegis_correlation_duration_seconds_bucket{le="0.01"} 5.0
aegis_correlation_duration_seconds_bucket{le="+Inf"} 5.0
aegis_correlation_duration_seconds_count 5.0
aegis_correlation_duration_seconds_sum 0.021
aegis_correlation_duration_seconds_created 1.7e+09
"""


def test_parse_keeps_total_suffix_from_sample_names():
    stats = parse_aegis_metrics(SAMPLE_EXPOSITION)
    assert stats["aegis_events_ingested_total"] == 42.0


def test_parse_filters_non_aegis_and_created_and_buckets():
    stats = parse_aegis_metrics(SAMPLE_EXPOSITION)
    for key in stats:
        assert key.startswith("aegis_")
        assert "_created" not in key
        assert "_bucket" not in key


def test_parse_keeps_gauges_and_histogram_count_sum():
    stats = parse_aegis_metrics(SAMPLE_EXPOSITION)
    assert stats["aegis_open_traces"] == 3.0
    assert stats["aegis_correlation_duration_seconds_count"] == 5.0
    assert stats["aegis_correlation_duration_seconds_sum"] == 0.021


def test_parse_labelled_sample_key_includes_labels():
    stats = parse_aegis_metrics(SAMPLE_EXPOSITION)
    key = "aegis_predicted_breach_eta_seconds{metric=connection_pool_usage,service=redis-cache}"
    assert stats[key] == 97.8


def test_war_room_whitelist_rejects_traversal():
    from app.main import WAR_ROOM_FILES
    assert "../../../etc/passwd" not in WAR_ROOM_FILES
    assert "incident_summary.md" in WAR_ROOM_FILES
    assert len(WAR_ROOM_FILES) == 6
