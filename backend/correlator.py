"""
Collects events into one bucket per trace_id, and decides when a bucket is
complete so it can be handed on as a single, time-sorted list.

The hard part: nothing tells us a trace is finished. Services just stop
logging. And events can arrive out of order. So we keep a "watermark", which
is a position on the events' own timeline: the newest event time seen so far
minus a small cushion. It only ever moves forward. Once the watermark is well
past a trace's newest event, that trace is done and we close it.

The watermark only moves when a new event arrives. If the last trace has no
traffic behind it, the watermark freezes and that trace would stay open
forever. So there is a backup rule: close a trace after enough real
(wall-clock) seconds of silence.
"""

import time

WATERMARK_GRACE_SECONDS = 10   # how far the watermark trails the newest event, to allow for late arrivals
IDLE_GAP_SECONDS = 15          # close a trace once the watermark is this far past its newest event
# A simulated episode lasts up to about 280s (see simulator.py). Keep this
# above that, or traces get cut off before the last events arrive.
MAX_TRACE_SECONDS = 320        # safety net: close any trace this long, no matter what
WALL_IDLE_SECONDS = 10         # backup rule: close a trace after this many real seconds of silence


class TraceBuffer:
    """All the events seen so far for one trace, plus when the first and last happened."""

    def __init__(self):
        self.events = []
        self.first_event_time = None
        self.last_event_time = None
        self.last_touched_wall = time.monotonic()

    def add(self, event):
        self.events.append(event)
        timestamp = event["timestamp"]
        if self.first_event_time is None or timestamp < self.first_event_time:
            self.first_event_time = timestamp
        if self.last_event_time is None or timestamp > self.last_event_time:
            self.last_event_time = timestamp
        self.last_touched_wall = time.monotonic()


class Correlator:
    """Holds every open trace and closes them when they are done."""

    def __init__(self):
        self.open_traces = {}
        self.closed_trace_ids = set()
        self.watermark = 0.0

    def add_event(self, event):
        trace_id = event["trace_id"]
        if trace_id in self.closed_trace_ids:
            # This trace was already closed, so the event is too late. Drop it.
            return

        if trace_id not in self.open_traces:
            self.open_traces[trace_id] = TraceBuffer()
        self.open_traces[trace_id].add(event)

        self._advance_watermark(event["timestamp"])

    def _advance_watermark(self, event_timestamp):
        # Never move backwards: a late, old event must not undo progress.
        candidate = event_timestamp - WATERMARK_GRACE_SECONDS
        if candidate > self.watermark:
            self.watermark = candidate

    def close_finished_traces(self):
        """Closes every trace that is finished. Returns them as a list of
        time-sorted event lists."""
        sealed = []
        for trace_id in list(self.open_traces.keys()):
            buffer = self.open_traces[trace_id]
            if self._is_finished(buffer):
                sealed.append(self._seal(trace_id))
        return sealed

    def _is_finished(self, buffer):
        # Rule 1: the timeline has moved on well past this trace's last event.
        idle_time = self.watermark - buffer.last_event_time
        # Rule 2: this trace has gone on too long (stops it growing forever).
        trace_age = buffer.last_event_time - buffer.first_event_time
        return idle_time >= IDLE_GAP_SECONDS or trace_age >= MAX_TRACE_SECONDS

    def _seal(self, trace_id):
        buffer = self.open_traces.pop(trace_id)
        self.closed_trace_ids.add(trace_id)
        return sorted(buffer.events, key=lambda event: event["timestamp"])

    def flush_all(self):
        """Closes every open trace right now, finished or not."""
        remaining_ids = list(self.open_traces.keys())
        return [self._seal(trace_id) for trace_id in remaining_ids]

    def close_wall_idle_traces(self):
        """Backup rule: closes traces that have received nothing for
        WALL_IDLE_SECONDS of real time, for when no new events are coming to
        move the watermark."""
        sealed = []
        now = time.monotonic()
        for trace_id in list(self.open_traces.keys()):
            buffer = self.open_traces[trace_id]
            if now - buffer.last_touched_wall >= WALL_IDLE_SECONDS:
                sealed.append(self._seal(trace_id))
        return sealed
