# Aegis RCA Eval Scorecard

| Metric | Score |
|--------|-------|
| Root cause top-1 match | 3/3 |
| Root cause class match | 3/3 |
| Schema validity | 3/3 |
| Degraded services match | 3/3 |

## redis_retry_storm.log
- Root: redis-cache (expected: redis-cache) PASS
- Class: PASS
- Edge basis: EVENT_TIME
- Traces/Events: 11/17

## pg_deadlock.log
- Root: postgres-db (expected: postgres-db) PASS
- Class: PASS
- Edge basis: SPAN
- Traces/Events: 2/13

## cache_stampede.log
- Root: postgres-db (expected: postgres-db) PASS
- Class: PASS
- Edge basis: SPAN
- Traces/Events: 5/18

