from app.scoreboard import Scoreboard
from app.predictor import Prediction


def _pred(kind, service, predicted_at, eta_seconds=60.0):
    return Prediction(service=service, metric="x", kind=kind, current_value=0.5, threshold=1.0,
                      eta_seconds=eta_seconds, predicted_at=predicted_at, confidence=0.9,
                      severity="WARNING", summary="test")


def test_hit_recorded_with_lead_time():
    sb = Scoreboard()
    sb.observe_predictions([_pred("TREND_BREACH", "redis-cache", predicted_at=100.0, eta_seconds=60.0)], now_ts=100.0)
    sb.observe_breach("redis-cache", breach_ts=150.0)
    snap = sb.snapshot()
    assert snap["STAT"]["hits"] == 1
    assert snap["STAT"]["mean_lead_seconds"] == 50.0


def test_false_alarm_when_no_breach_before_expiry():
    sb = Scoreboard()
    sb.observe_predictions([_pred("ML_RISK", "redis-cache", predicted_at=100.0, eta_seconds=10.0)], now_ts=100.0)
    sb.observe_predictions([], now_ts=100.0 + 10.0 + 30.0 + 1.0)
    snap = sb.snapshot()
    assert snap["ML"]["false_alarms"] == 1
    assert snap["ML"]["hits"] == 0


def test_first_to_fire_when_both_sources_predict():
    sb = Scoreboard()
    sb.observe_predictions([_pred("TREND_BREACH", "redis-cache", predicted_at=90.0, eta_seconds=60.0)], now_ts=90.0)
    sb.observe_predictions([_pred("ML_RISK", "redis-cache", predicted_at=100.0, eta_seconds=60.0)], now_ts=100.0)
    sb.observe_breach("redis-cache", breach_ts=150.0)
    snap = sb.snapshot()
    assert snap["STAT"]["first_to_fire"] == 1
    assert snap["ML"]["first_to_fire"] == 0
