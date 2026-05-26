# 🕒 Deterministic Incident Trace Timeline

This chronological timeline has been mapped from trace events correlated via shared trace/request tokens:

| Timestamp | Service | Severity | Event Message |
| :--- | :--- | :--- | :--- |
| `10:41:00` | **api-gateway** | ℹ️ `INFO` | Received checkout request |
| `10:41:00` | **checkout-service** | ℹ️ `INFO` | Processing checkout items, executing transaction |
| `10:41:00` | **postgres-db** | ℹ️ `INFO` |  |
| `10:41:00` | **checkout-service** | ℹ️ `INFO` | Enqueuing payment task to Celery |