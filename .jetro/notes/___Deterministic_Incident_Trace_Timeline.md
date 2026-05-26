# 🕒 Deterministic Incident Trace Timeline

Chronological timeline mapped from trace events correlated via shared trace/request tokens:

| Timestamp | Service | Severity | Event Message |
| :--- | :--- | :--- | :--- |
| `16:20:00` | **rate-limiter** | ℹ️ `INFO` | Rate limit verification success |
| `16:20:00` | **api-gateway** | ℹ️ `INFO` | Routing popular products request |
| `16:20:00` | **product-service** | ℹ️ `INFO` | Cache hit on popular products |