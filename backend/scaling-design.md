# Aegis Scaling Design

## Current Architecture
- Single Kafka consumer in group `aegis-correlators` processes all partitions
- Events keyed by `trace_id` → correlation state is partition-local
- Single consumer handles 17-event scenarios in <3s with zero lag

## When to Scale (Gate Conditions)
K8s + KEDA autoscaling is justified ONLY when ALL of these hold:
1. `kafka_consumergroup_lag` consistently > 1000 under sustained production load
2. Adding a second consumer measurably reduces lag (verified via `docker-compose --scale consumer=2`)
3. Lag growth rate exceeds single-consumer drain rate by >2x

## How to Scale (if gate passes)
1. **Consumer Deployment**: K8s `Deployment` with `replicas: 1` (KEDA manages scaling)
2. **KEDA ScaledObject**: Scale on `kafka_consumergroup_lag` for topic `telemetry.raw`
   - `minReplicaCount: 1`
   - `maxReplicaCount`: number of partitions (scaling beyond partition count is useless)
   - `lagThreshold: 500` (tune based on observed drain rate)
3. **Partition count**: Must match expected max consumers (e.g., 6 partitions → max 6 consumers)
4. **Key insight preserved**: Keying by `trace_id` keeps correlation partition-local.
   No cross-consumer coordination. Adding a consumer just reassigns partitions.

## Why NOT Now
- Sample workload: 17 events/scenario, sub-second processing
- Consumer idle >99% of wall-clock time between events
- Zero observed lag under any replay rate tested
- Single-pod K8s with no scaling signal = infrastructure theater

## What Would Change This
- Production deployment ingesting real telemetry (>1000 events/sec sustained)
- Multiple concurrent incident streams from instrumented microservices
- Consumer processing time dominated by I/O (external DB writes, API calls)
