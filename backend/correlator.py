"""
Buffers events per trace_id until a trace is safe to consider complete, then
hands it off as one time-sorted list.

Events can arrive out of order, so we track a "watermark": the newest event
time seen so far, minus a grace period. The watermark only moves forward.
Once it has passed a trace's last event by enough, we know that trace is
done and seal it.

The watermark only advances when a new event arrives, so an isolated trace
with no further traffic behind it would never close on event-time alone.
close_wall_idle_traces() covers that: it closes a trace once real (wall
clock) time has passed since we last touched it, regardless of the
synthetic event timestamps.
"""

import time

WATERMARK_GRACE_SECONDS = 10   # cushion before trusting the newest event time
IDLE_GAP_SECONDS = 15          # close a trace once the watermark passes it by this much
# A full simulated episode (baseline, warning, retry backoff, cascade -- see
# simulator.py) can span up to ~280s. This must stay comfortably above that,
# or the safety net fires mid-episode and cuts off events still to come.
MAX_TRACE_SECONDS = 320        # safety net: close a trace after this long regardless
WALL_IDLE_SECONDS = 10         # close a trace after this many real seconds of silence


class TraceBuffer:
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
    def __init__(self):
        self.open_traces = {}
        self.closed_trace_ids = set()
        self.watermark = 0.0

    def add_event(self, event):
        trace_id = event["trace_id"]
        if trace_id in self.closed_trace_ids:
            # Trace already sealed; this event is too late to include.
            return

        if trace_id not in self.open_traces:
            self.open_traces[trace_id] = TraceBuffer()
        self.open_traces[trace_id].add(event)

        self._advance_watermark(event["timestamp"])

    def _advance_watermark(self, event_timestamp):
        candidate = event_timestamp - WATERMARK_GRACE_SECONDS
        if candidate > self.watermark:
            self.watermark = candidate

    def close_finished_traces(self):
        """Returns sealed traces (each a time-sorted list of events) for
        every open trace that's now finished."""
        sealed = []
        for trace_id in list(self.open_traces.keys()):
            buffer = self.open_traces[trace_id]
            if self._is_finished(buffer):
                sealed.append(self._seal(trace_id))
        return sealed

    def _is_finished(self, buffer):
        idle_time = self.watermark - buffer.last_event_time
        trace_age = buffer.last_event_time - buffer.first_event_time
        return idle_time >= IDLE_GAP_SECONDS or trace_age >= MAX_TRACE_SECONDS

    def _seal(self, trace_id):
        buffer = self.open_traces.pop(trace_id)
        self.closed_trace_ids.add(trace_id)
        return sorted(buffer.events, key=lambda event: event["timestamp"])

    def flush_all(self):
        """Force-closes every remaining open trace, e.g. at shutdown."""
        remaining_ids = list(self.open_traces.keys())
        return [self._seal(trace_id) for trace_id in remaining_ids]

    def close_wall_idle_traces(self):
        """Closes any trace that hasn't received a new event in the last
        WALL_IDLE_SECONDS of real time -- the fallback for a trace with no
        later traffic to advance the event-time watermark past it."""
        sealed = []
        now = time.monotonic()
        for trace_id in list(self.open_traces.keys()):
            buffer = self.open_traces[trace_id]
            if now - buffer.last_touched_wall >= WALL_IDLE_SECONDS:
                sealed.append(self._seal(trace_id))
        return sealed
