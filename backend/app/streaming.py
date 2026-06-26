from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional


WINDOW_MAX_SECONDS = 90.0
IDLE_GAP_SECONDS = 15.0
GRACE_PERIOD_SECONDS = 10.0
MAX_OPEN_TRACES = 5000
MAX_CLOSED_MEMORY = 1000
INCIDENT_WINDOW_SECONDS = 120.0


def parse_event_time(event: dict) -> Optional[float]:
    ts = event.get("timestamp")
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp()
    except (ValueError, AttributeError):
        return None


def dedupe_key(event: dict) -> tuple:
    trace_id = event.get("trace_id")
    span_id = event.get("span_id")
    timestamp = event.get("timestamp")
    if span_id is not None:
        return (trace_id, span_id, timestamp)
    return (trace_id, hash(event.get("raw_log", "")), timestamp)


@dataclass
class TraceBuffer:
    trace_id: str
    events: list = field(default_factory=list)
    seen_keys: set = field(default_factory=set)
    min_ts: float = float("inf")
    max_ts: float = float("-inf")


@dataclass
class EmittedTrace:
    trace_id: str
    events: list
    min_ts: float
    max_ts: float


class StreamingCorrelator:
    def __init__(self, window_max=WINDOW_MAX_SECONDS,
                 idle_gap=IDLE_GAP_SECONDS,
                 grace=GRACE_PERIOD_SECONDS,
                 max_open=MAX_OPEN_TRACES,
                 wall_idle=None):
        self.window_max = window_max
        self.idle_gap = idle_gap
        self.grace = grace
        self.max_open = max_open
        self.wall_idle = wall_idle
        self._buffers = OrderedDict()
        self._watermark = float("-inf")
        self._closed_ids = set()
        self._late_count = 0

    def ingest(self, event: dict) -> List[EmittedTrace]:
        et = parse_event_time(event)
        event["_event_ts"] = et

        trace_id = event.get("trace_id")
        if not trace_id:
            return []

        if trace_id in self._closed_ids:
            self._late_count += 1
            return []

        key = dedupe_key(event)
        buf = self._buffers.get(trace_id)
        if buf is not None and key in buf.seen_keys:
            return []

        closed = []
        if buf is None:
            if len(self._buffers) >= self.max_open:
                closed.extend(self._evict_oldest())
            buf = TraceBuffer(trace_id=trace_id)
            self._buffers[trace_id] = buf

        buf.events.append(event)
        buf.seen_keys.add(key)
        if et is not None:
            buf.min_ts = min(buf.min_ts, et)
            buf.max_ts = max(buf.max_ts, et)

        self._buffers.move_to_end(trace_id)

        if et is not None:
            self._watermark = max(self._watermark, et - self.grace)

        closed.extend(self._collect_closures())
        return closed

    def tick(self) -> List[EmittedTrace]:
        return self._collect_closures()

    def flush_all(self) -> List[EmittedTrace]:
        result = []
        for trace_id in list(self._buffers.keys()):
            result.append(self._seal(trace_id))
        return result

    @property
    def late_count(self) -> int:
        return self._late_count

    @property
    def open_trace_count(self) -> int:
        return len(self._buffers)

    def _should_close(self, buf: TraceBuffer) -> bool:
        if buf.max_ts == float("-inf"):
            return False
        if (self._watermark - buf.max_ts) >= self.idle_gap:
            return True
        if (buf.max_ts - buf.min_ts) >= self.window_max:
            return True
        return False

    def _collect_closures(self) -> List[EmittedTrace]:
        closed = []
        for trace_id in list(self._buffers.keys()):
            if self._should_close(self._buffers[trace_id]):
                closed.append(self._seal(trace_id))
        return closed

    def _seal(self, trace_id: str) -> EmittedTrace:
        buf = self._buffers.pop(trace_id)
        buf.events.sort(key=lambda e: (e.get("_event_ts") or 0.0, e.get("span_id") or ""))
        self._remember_closed(trace_id)
        return EmittedTrace(
            trace_id=trace_id,
            events=buf.events,
            min_ts=buf.min_ts,
            max_ts=buf.max_ts,
        )

    def _evict_oldest(self) -> List[EmittedTrace]:
        oldest_id = next(iter(self._buffers))
        return [self._seal(oldest_id)]

    def _remember_closed(self, trace_id: str):
        self._closed_ids.add(trace_id)
        if len(self._closed_ids) > MAX_CLOSED_MEMORY:
            self._closed_ids.pop()


class IncidentAssembler:
    def __init__(self, window=INCIDENT_WINDOW_SECONDS):
        self.window = window
        self._bucket_start = None
        self._bucket_traces = []

    def add(self, trace: EmittedTrace) -> Optional[List[dict]]:
        if self._bucket_start is None:
            self._bucket_start = trace.min_ts
            self._bucket_traces.append(trace)
            return None

        if trace.min_ts >= self._bucket_start + self.window:
            result = self._finalize()
            self._bucket_start = trace.min_ts
            self._bucket_traces.append(trace)
            return result

        self._bucket_traces.append(trace)
        return None

    def flush(self) -> Optional[List[dict]]:
        if not self._bucket_traces:
            return None
        return self._finalize()

    def _finalize(self) -> List[dict]:
        all_events = []
        for trace in self._bucket_traces:
            all_events.extend(trace.events)
        all_events.sort(key=lambda e: (e.get("_event_ts") or 0.0, e.get("span_id") or ""))
        self._bucket_traces = []
        self._bucket_start = None
        return all_events
