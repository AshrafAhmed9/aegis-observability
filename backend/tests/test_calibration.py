import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("sklearn")
pytest.importorskip("pandas")

from ml.train_failure_model import calibration_curve, expected_calibration_error, brier_score


def test_perfectly_calibrated_gives_near_zero_ece():
    rng = np.random.RandomState(0)
    n = 5000
    y_proba = rng.uniform(0.0, 1.0, n)
    # Ground truth generated FROM the predicted probabilities themselves:
    # a model that is exactly as confident as it should be.
    y_true = (rng.uniform(0.0, 1.0, n) < y_proba).astype(int)
    curve = calibration_curve(y_true, y_proba, n_bins=10)
    ece = expected_calibration_error(curve, len(y_true))
    assert ece < 0.03


def test_overconfident_model_gives_high_ece():
    rng = np.random.RandomState(0)
    n = 2000
    # Model always says 95% confident, but is only actually right half the time.
    y_proba = np.full(n, 0.95)
    y_true = (rng.uniform(0, 1, n) < 0.5).astype(int)
    curve = calibration_curve(y_true, y_proba, n_bins=10)
    ece = expected_calibration_error(curve, len(y_true))
    assert ece > 0.3


def test_brier_score_zero_for_perfect_predictions():
    y_true = np.array([0, 1, 1, 0])
    y_proba = np.array([0.0, 1.0, 1.0, 0.0])
    assert brier_score(y_true, y_proba) == 0.0


def test_brier_score_worst_case_is_one():
    y_true = np.array([0, 1])
    y_proba = np.array([1.0, 0.0])
    assert brier_score(y_true, y_proba) == 1.0


def test_calibration_curve_empty_bins_are_skipped():
    y_true = np.array([1, 1, 1])
    y_proba = np.array([0.95, 0.96, 0.97])
    curve = calibration_curve(y_true, y_proba, n_bins=10)
    # only the 0.9-1.0 bin should have data
    assert len(curve) == 1
    assert curve[0]["count"] == 3
