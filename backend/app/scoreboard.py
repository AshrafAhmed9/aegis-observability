"""Live champion/challenger scoreboard.

Tracks, per prediction source (STAT vs ML), how predictions resolve
against real outcomes as they happen in the live stream:
  - hit: the target service went ERROR/CRITICAL while a prediction was
    pending (or shortly after it expired)
  - false_alarm: a prediction's window elapsed with no breach
  - first_to_fire: when both sources predicted the same service, which
    predicted first

Pure bookkeeping on data already flowing through the live pipeline — no
new external dependencies.
"""
from typing import Dict, Optional

OUTCOME_GRACE_SECONDS = 30.0


class _Pending:
    __slots__ = ("service", "predicted_at", "expires_at")

    def __init__(self, service, predicted_at, expires_at):
        self.service = service
        self.predicted_at = predicted_at
        self.expires_at = expires_at


class Scoreboard:
    def __init__(self):
        self._pending: Dict[str, Dict[str, _Pending]] = {"STAT": {}, "ML": {}}
        self._stats = {
            "STAT": {"hits": 0, "false_alarms": 0, "first_to_fire": 0, "lead_times": []},
            "ML": {"hits": 0, "false_alarms": 0, "first_to_fire": 0, "lead_times": []},
        }

    def _source_of(self, kind: str) -> Optional[str]:
        if kind == "ML_RISK":
            return "ML"
        if kind in ("TREND_BREACH", "ANOMALY", "ERROR_RATE"):
            return "STAT"
        return None

    def observe_predictions(self, predictions, now_ts: float) -> None:
        for p in predictions:
            source = self._source_of(p.kind)
            if source is None:
                continue
            if p.service not in self._pending[source]:
                ttl = (p.eta_seconds or 120.0) + OUTCOME_GRACE_SECONDS
                self._pending[source][p.service] = _Pending(p.service, p.predicted_at, p.predicted_at + ttl)

        for source in ("STAT", "ML"):
            expired = [svc for svc, pend in self._pending[source].items() if now_ts > pend.expires_at]
            for svc in expired:
                del self._pending[source][svc]
                self._stats[source]["false_alarms"] += 1

    def observe_breach(self, service: str, breach_ts: float) -> None:
        fired_at = {}
        for source in ("STAT", "ML"):
            pend = self._pending[source].pop(service, None)
            if pend is not None:
                self._stats[source]["hits"] += 1
                self._stats[source]["lead_times"].append(breach_ts - pend.predicted_at)
                fired_at[source] = pend.predicted_at
        if len(fired_at) == 2:
            winner = min(fired_at, key=fired_at.get)
            self._stats[winner]["first_to_fire"] += 1

    def snapshot(self) -> dict:
        out = {}
        for source, s in self._stats.items():
            leads = s["lead_times"]
            out[source] = {
                "hits": s["hits"],
                "false_alarms": s["false_alarms"],
                "first_to_fire": s["first_to_fire"],
                "mean_lead_seconds": (sum(leads) / len(leads)) if leads else None,
            }
        return out
