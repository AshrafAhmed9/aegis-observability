import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

@dataclass(frozen=True)
class MetricSpec:
    threshold: float
    rearm_below: float
    min_points: int = 4

TRACKED_METRICS: dict[str, MetricSpec] = {
    "connection_pool_usage": MetricSpec(threshold=1.0, rearm_below=0.8),
    "queue_depth": MetricSpec(threshold=500.0, rearm_below=200.0),
    "latency_ms": MetricSpec(threshold=2000.0, rearm_below=1000.0),
    "active_connections": MetricSpec(threshold=200.0, rearm_below=150.0),
}

HORIZON_SECONDS = 600.0
PREDICTION_TTL_SECONDS = 120.0
Z_THRESHOLD = 3.0
MIN_R2 = 0.6
ERROR_RATE_WINDOW_SECONDS = 30.0

TREND_BREACH = "TREND_BREACH"
ANOMALY = "ANOMALY"
ERROR_RATE = "ERROR_RATE"

@dataclass
class Prediction:
    service: str
    metric: str
    kind: str
    current_value: float
    threshold: Optional[float]
    eta_seconds: Optional[float]
    predicted_at: float
    confidence: float
    severity: str
    summary: str

    def to_dict(self) -> dict:
        return {
            "service": self.service,
            "metric": self.metric,
            "kind": self.kind,
            "current_value": self.current_value,
            "threshold": self.threshold,
            "eta_seconds": self.eta_seconds,
            "predicted_at": self.predicted_at,
            "confidence": round(self.confidence, 3),
            "severity": self.severity,
            "summary": self.summary,
        }

class MetricWindow:
    """Rolling event-time state for one (service, metric) pair."""

    def __init__(self, maxlen: int = 60, alpha: float = 0.3):
        self.alpha = alpha
        self.points: deque = deque(maxlen=maxlen)
        self.ewma_mean: Optional[float] = None
        self.ewma_var: float = 0.0

    def add(self, ts: float, value: float) -> None:
        self.points.append((ts, value))
        if self.ewma_mean is None:
            self.ewma_mean = value
            self.ewma_var = 0.0
        else:
            diff = value - self.ewma_mean
            incr = self.alpha * diff
            self.ewma_mean += incr
            self.ewma_var = (1 - self.alpha) * (self.ewma_var + diff * incr)

    def zscore(self, value: float) -> Optional[float]:
        if self.ewma_mean is None or len(self.points) < 4:
            return None
        std = self.ewma_var ** 0.5
        if std < 1e-9:
            return None
        return (value - self.ewma_mean) / std

    def slope_and_r2(self) -> Optional[tuple]:
        n = len(self.points)
        if n < 4:
            return None
        xs = [p[0] for p in self.points]
        ys = [p[1] for p in self.points]
        x0 = xs[0]
        xs = [x - x0 for x in xs]  # numerical stability

        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        sxx = sum((x - mean_x) ** 2 for x in xs)
        sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        if sxx < 1e-9:
            return None
        slope = sxy / sxx
        intercept = mean_y - slope * mean_x

        ss_tot = sum((y - mean_y) ** 2 for y in ys)
        if ss_tot < 1e-9:
            return (slope, 1.0 if abs(slope) < 1e-9 else 0.0)
        ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
        r2 = 1.0 - (ss_res / ss_tot)
        return (slope, r2)

    @property
    def last_value(self) -> Optional[float]:
        return self.points[-1][1] if self.points else None

    @property
    def last_ts(self) -> Optional[float]:
        return self.points[-1][0] if self.points else None

class FailurePredictor:
    def __init__(self, horizon_seconds: float = HORIZON_SECONDS,
                 prediction_ttl: float = PREDICTION_TTL_SECONDS,
                 z_threshold: float = Z_THRESHOLD,
                 min_r2: float = MIN_R2):
        self.horizon_seconds = horizon_seconds
        self.prediction_ttl = prediction_ttl
        self.z_threshold = z_threshold
        self.min_r2 = min_r2
        self._windows: dict = {}          # (service, metric) -> MetricWindow
        self._active: dict = {}           # (service, metric, kind) -> Prediction
        self._error_events: dict = {}     # service -> deque[ts]
        self._watermark = float("-inf")

    def observe(self, event: dict) -> list:
        ts = event.get("_event_ts")
        service = event.get("service")
        if ts is None or not service:
            return []
        self._watermark = max(self._watermark, ts)

        emitted = []
        for metric, spec in TRACKED_METRICS.items():
            value = event.get(metric)
            if value is None:
                continue
            key = (service, metric)
            window = self._windows.setdefault(key, MetricWindow())
            window.add(ts, float(value))
            emitted.extend(self._evaluate_metric(service, metric, spec, window, ts))

        severity = event.get("severity")
        if severity in ("ERROR", "CRITICAL"):
            emitted.extend(self._evaluate_error_rate(service, ts))

        return emitted

    def _evaluate_metric(self, service, metric, spec, window, ts) -> list:
        emitted = []
        current = window.last_value
        key = (service, metric, TREND_BREACH)

        fit = window.slope_and_r2()
        if fit is not None:
            slope, r2 = fit
            if (slope > 1e-9 and r2 >= self.min_r2
                    and current is not None and current < spec.threshold):
                eta = (spec.threshold - current) / slope
                if 0 < eta <= self.horizon_seconds:
                    pred = Prediction(
                        service=service, metric=metric, kind=TREND_BREACH,
                        current_value=current, threshold=spec.threshold,
                        eta_seconds=eta, predicted_at=ts, confidence=r2,
                        severity="CRITICAL" if eta <= 120 else "WARNING",
                        summary=f"{service} {metric} will cross {spec.threshold:g} in ~{int(eta)}s",
                    )
                    self._active[key] = pred
                    emitted.append(pred)
            elif current is not None and current <= spec.rearm_below:
                self._active.pop(key, None)

        z_key = (service, metric, ANOMALY)
        z = window.zscore(current) if current is not None else None
        if z is not None and abs(z) >= self.z_threshold:
            pred = Prediction(
                service=service, metric=metric, kind=ANOMALY,
                current_value=current, threshold=None, eta_seconds=None,
                predicted_at=ts, confidence=min(1.0, abs(z) / (self.z_threshold * 2)),
                severity="WARNING",
                summary=f"{service} {metric} is anomalous (z={z:.1f})",
            )
            self._active[z_key] = pred
            emitted.append(pred)

        return emitted

    def _evaluate_error_rate(self, service, ts) -> list:
        dq = self._error_events.setdefault(service, deque())
        dq.append(ts)
        while dq and ts - dq[0] > ERROR_RATE_WINDOW_SECONDS * 2:
            dq.popleft()

        recent = sum(1 for t in dq if ts - t <= ERROR_RATE_WINDOW_SECONDS)
        prior = sum(1 for t in dq if ERROR_RATE_WINDOW_SECONDS < ts - t <= ERROR_RATE_WINDOW_SECONDS * 2)

        key = (service, "error_rate", ERROR_RATE)
        if recent >= 3 and recent > prior:
            pred = Prediction(
                service=service, metric="error_rate", kind=ERROR_RATE,
                current_value=float(recent), threshold=None, eta_seconds=None,
                predicted_at=ts, confidence=min(1.0, recent / 10.0),
                severity="CRITICAL" if recent >= 6 else "WARNING",
                summary=f"{service} error rate rising ({recent} errors in {int(ERROR_RATE_WINDOW_SECONDS)}s)",
            )
            self._active[key] = pred
            return [pred]
        return []

    def active(self) -> list:
        now = self._watermark
        live = []
        for key, pred in list(self._active.items()):
            age = now - pred.predicted_at
            if pred.eta_seconds is not None and age > pred.eta_seconds:
                del self._active[key]
                continue
            if age > self.prediction_ttl:
                del self._active[key]
                continue
            live.append(pred)
        return live
