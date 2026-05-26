# 🕸️ Failure Propagation Graph

This flowchart visualizes how cascading database latencies and lock contentions propagated through downstream service nodes to cause the system-wide gateway meltdown:

```mermaid
flowchart TD
    api_gateway["api-gateway (INFO)"]
    style api_gateway fill:#f0fdf4,stroke:#16a34a,stroke-width:1px,color:#166534
    checkout_service["checkout-service (INFO)"]
    style checkout_service fill:#f0fdf4,stroke:#16a34a,stroke-width:1px,color:#166534
    postgres_db["postgres-db (INFO)"]
    style postgres_db fill:#f0fdf4,stroke:#16a34a,stroke-width:1px,color:#166534
    api_gateway --> checkout_service
    checkout_service --> postgres_db

```

> [!TIP]
> Red boxes indicate failed or severely degraded service components. Connective arrow tags trace the propagation vector and error classes that traversed network layers.