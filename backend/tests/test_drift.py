from ml import DriftMonitor, FEATURE_NAMES

TRAINING_DISTRIBUTION = {
    name: {"p10": 0.0, "p50": 1.0, "p90": 2.0} for name in FEATURE_NAMES
}


def test_status_with_no_observations_is_ok():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    status = monitor.status()
    assert status["max_psi"] == 0.0
    assert status["level"] == "OK"


def test_observe_records_values_per_feature():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    monitor.observe([1, 1, 1, 1])
    assert len(monitor.observed_values["warning_count"]) == 1


def test_psi_is_zero_with_fewer_than_ten_samples():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    for _ in range(5):
        monitor.observe([1, 1, 1, 1])
    assert monitor._psi_for("warning_count") == 0.0


def test_psi_is_low_when_live_data_matches_training_distribution():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    # Reproduce the training buckets' expected shares (10/40/40/10) instead of
    # a single repeated value, which would itself look like a shifted
    # distribution even if it's "close" to the median.
    values = [-1.0] * 5 + [0.5] * 20 + [1.5] * 20 + [3.0] * 5
    for value in values:
        monitor.observe([value, value, value, value])
    assert monitor.status()["level"] == "OK"


def test_psi_is_high_when_live_data_has_shifted():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    for _ in range(50):
        monitor.observe([100, 100, 100, 100])  # far outside the training range
    assert monitor.status()["level"] == "ALERT"


def test_bucket_shares_sum_to_one():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    shares = monitor._bucket_shares([0.0, 1.0, 1.5, 3.0], [0.0, 1.0, 2.0])
    assert abs(sum(shares) - 1.0) < 1e-9


def test_status_reports_every_feature():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    status = monitor.status()
    assert set(status["features"].keys()) == set(FEATURE_NAMES)


def test_psi_calculation_does_not_crash_on_empty_bucket():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    for _ in range(20):
        monitor.observe([0.0, 0.0, 0.0, 0.0])  # all in the lowest bucket, others empty
    assert isinstance(monitor.status()["max_psi"], float)


def test_level_thresholds_are_ordered():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    assert monitor._level_for(0.05) == "OK"
    assert monitor._level_for(0.15) == "WARN"
    assert monitor._level_for(0.30) == "ALERT"


def test_drifted_feature_does_not_hide_behind_a_stable_one():
    monitor = DriftMonitor(TRAINING_DISTRIBUTION)
    for _ in range(50):
        monitor.observe([1, 100, 1, 1])  # only error_count has drifted
    status = monitor.status()
    assert status["features"]["error_count"] > status["features"]["warning_count"]
