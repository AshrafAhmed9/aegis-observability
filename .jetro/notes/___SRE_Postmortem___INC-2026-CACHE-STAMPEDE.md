# 📝 SRE Incident Postmortem — INC-2026-CACHE-STAMPEDE

| Owner | Status | Target Fix | Review Date |
| :--- | :--- | :--- | :--- |
| **SRE Platform Team** | `RESOLVED` | `MERGED` | **2026-05-28** |

## 1. Executive Summary
Cache key expiration under un-throttled request spikes led to database connection pool starvation. Upstream checkout gateways failed with Service Unavailable errors.

## 2. Root Cause Analysis (RCA)
* **Core Cause:** `Cache Miss Stampede (Cache Thundering Herd)`
* **Confidence Rating:** `96%`
* **Root Diagnostic:** The popular products cache key expired, causing all concurrent API requests to miss the cache simultaneously and dispatch expensive database query joins, flooding the connection pool.

## 3. Preventive Action Items
- [x] **Implement a single-flight lock mechanism for high-demand cache key re-population.**
- [x] **Introduce jitter to cache TTL values to prevent simultaneous multi-key invalidation.**
- [x] **Upgrade the rate-limiter sync heartbeat frequency to reduce local bypass windows.**