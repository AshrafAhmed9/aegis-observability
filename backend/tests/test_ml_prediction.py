import ml
import simulator
from train import measure_lead_times, median, rows_for_episodes, split_episodes, train_model


def test_extract_features_returns_one_value_per_feature_name():
    features = ml.extract_features([], {"service": "api", "timestamp": 0})
    assert len(features) == len(ml.FEATURE_NAMES)


def test_warning_count_only_counts_same_service():
    prior = [
        {"service": "api", "severity": "WARNING", "timestamp": 0},
        {"service": "cache", "severity": "WARNING", "timestamp": 1},
    ]
    current = {"service": "api", "timestamp": 2}
    warning_count = ml.extract_features(prior, current)[0]
    assert warning_count == 1


def test_error_count_only_counts_error_and_critical():
    prior = [
        {"service": "api", "severity": "WARNING", "timestamp": 0},
        {"service": "api", "severity": "ERROR", "timestamp": 1},
    ]
    current = {"service": "api", "timestamp": 2}
    error_count = ml.extract_features(prior, current)[1]
    assert error_count == 1


def test_seconds_since_last_event_is_zero_with_no_history():
    features = ml.extract_features([], {"service": "api", "timestamp": 5})
    assert features[2] == 0.0


def test_seconds_since_last_event_measures_the_gap():
    prior = [{"service": "api", "severity": "INFO", "timestamp": 3}]
    current = {"service": "api", "timestamp": 10}
    features = ml.extract_features(prior, current)
    assert features[2] == 7.0


def test_degraded_services_count_is_global_not_per_service():
    prior = [{"service": "cache", "severity": "ERROR", "timestamp": 0}]
    current = {"service": "api", "timestamp": 1}
    degraded_count = ml.extract_features(prior, current)[3]
    assert degraded_count == 1


def test_build_training_rows_has_exactly_one_positive_per_episode():
    events, root_cause_service, _, _ = simulator.generate_episode(seed=1, fault_name="redis_leak")
    episode = {"events": events, "root_cause_service": root_cause_service}
    rows = ml.build_training_rows(episode)
    positive_rows = [row for row in rows if row[1] == 1]
    assert len(positive_rows) == 1


def test_build_training_rows_has_negative_rows_from_other_services():
    events, root_cause_service, _, _ = simulator.generate_episode(seed=1, fault_name="redis_leak")
    episode = {"events": events, "root_cause_service": root_cause_service}
    rows = ml.build_training_rows(episode)
    negative_rows = [row for row in rows if row[1] == 0]
    assert len(negative_rows) > 0


def test_compute_feature_distribution_has_percentiles_per_feature():
    rows = [[1, 0, 5.0, 0], [2, 0, 6.0, 1], [3, 1, 7.0, 1]]
    distribution = ml.compute_feature_distribution(rows)
    for name in ml.FEATURE_NAMES:
        assert set(distribution[name].keys()) == {"p10", "p50", "p90"}


def test_failure_detector_load_returns_none_when_files_missing(tmp_path):
    detector = ml.FailureDetector.load(tmp_path / "missing_model.joblib", tmp_path / "missing_dist.json")
    assert detector is None


def _train_tiny_detector():
    # Uses the same train/val split and full training set as train.py, so
    # the model has enough data to separate the two classes confidently.
    train_episodes, val_episodes = split_episodes()
    rows = rows_for_episodes(train_episodes)
    model = train_model(rows)
    distribution = ml.compute_feature_distribution([f for f, _ in rows])
    return ml.FailureDetector(model, distribution), val_episodes


def test_predict_risk_returns_a_probability():
    detector, _ = _train_tiny_detector()
    risk = detector.predict_risk([1, 0, 5.0, 0])
    assert 0.0 <= risk <= 1.0


def test_is_high_risk_matches_the_threshold():
    detector, _ = _train_tiny_detector()
    features = [1, 0, 5.0, 0]
    risk = detector.predict_risk(features)
    assert detector.is_high_risk(features) == (risk >= ml.RISK_THRESHOLD)


def test_trained_model_flags_a_clear_warning_as_high_risk():
    detector, _ = _train_tiny_detector()
    # The gap since this service's last event is what marks a WARNING moment
    # -- warning_count itself is always 0 here, since a row is built from
    # events *before* the one being classified (see extract_features).
    assert detector.is_high_risk([0, 0, 20.0, 0])


def test_trained_model_does_not_flag_healthy_baseline_traffic():
    detector, _ = _train_tiny_detector()
    # A short, regular gap between events -- typical of routine baseline traffic.
    assert not detector.is_high_risk([0, 0, 5.0, 0])


def test_median_helper_handles_odd_and_even_length_lists():
    assert median([1, 2, 3]) == 2
    assert median([1, 2, 3, 4]) == 2.5


def test_split_episodes_train_and_val_are_disjoint():
    train_episodes, val_episodes = split_episodes()
    train_seeds = {e["seed"] for e in train_episodes}
    val_seeds = {e["seed"] for e in val_episodes}
    assert train_seeds.isdisjoint(val_seeds)
