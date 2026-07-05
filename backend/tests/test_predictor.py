from app.predictor import FailurePredictor, TREND_BREACH, ANOMALY


def _ev(service, ts_offset, **fields):
    ts = 1748429260.0 + ts_offset
    ev = {"service": service, "_event_ts": ts}
    ev.update(fields)
    return ev


def test_trend_breach_predicted_before_threshold():
    p = FailurePredictor()
    # linear ramp 0.5 -> 0.9 over 40s, threshold is 1.0
    for i in range(8):
        p.observe(_ev("redis-cache", i * 5, connection_pool_usage=0.5 + i * 0.05))
    preds = [pr for pr in p.active() if pr.kind == TREND_BREACH]
    assert preds, "expected a trend breach prediction"
    pred = preds[0]
    assert pred.eta_seconds is not None
    assert pred.eta_seconds > 0
    assert 0.6 <= pred.confidence <= 1.0


def test_flat_series_no_trend_prediction():
    p = FailurePredictor()
    for i in range(8):
        p.observe(_ev("redis-cache", i * 5, connection_pool_usage=0.5))
    preds = [pr for pr in p.active() if pr.kind == TREND_BREACH]
    assert preds == []


def test_noisy_flat_series_no_trend_but_may_anomaly():
    p = FailurePredictor()
    values = [0.5, 0.51, 0.49, 0.52, 0.48, 0.50, 0.51, 0.49]
    for i, v in enumerate(values):
        p.observe(_ev("redis-cache", i * 5, connection_pool_usage=v))
    preds = [pr for pr in p.active() if pr.kind == TREND_BREACH]
    assert preds == []


def test_recovery_clears_trend_prediction():
    p = FailurePredictor()
    for i in range(8):
        p.observe(_ev("redis-cache", i * 5, connection_pool_usage=0.5 + i * 0.05))
    assert any(pr.kind == TREND_BREACH for pr in p.active())
    # drop back under rearm_below (0.8)
    for i in range(8, 12):
        p.observe(_ev("redis-cache", i * 5, connection_pool_usage=0.3))
    assert not any(pr.kind == TREND_BREACH for pr in p.active())


def test_rate_independence_same_deltas_same_prediction():
    p1 = FailurePredictor()
    p2 = FailurePredictor()
    base_values = [0.5 + i * 0.05 for i in range(8)]
    for i, v in enumerate(base_values):
        p1.observe(_ev("redis-cache", i * 5, connection_pool_usage=v))
        p2.observe(_ev("redis-cache", i * 5, connection_pool_usage=v))
    pred1 = [pr for pr in p1.active() if pr.kind == TREND_BREACH][0]
    pred2 = [pr for pr in p2.active() if pr.kind == TREND_BREACH][0]
    assert abs(pred1.eta_seconds - pred2.eta_seconds) < 1e-6


def test_dedup_one_active_per_service_metric_kind():
    p = FailurePredictor()
    for i in range(10):
        p.observe(_ev("redis-cache", i * 5, connection_pool_usage=0.5 + i * 0.04))
    preds = [pr for pr in p.active() if pr.kind == TREND_BREACH]
    assert len(preds) == 1
