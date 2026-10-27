#!/usr/bin/env bash
# One-command demo: brings up Kafka/Prometheus/Grafana and the API, then
# injects a fault so there's an incident to look at immediately. Ctrl+C
# tears everything down.
set -euo pipefail
cd "$(dirname "$0")"

PIDS=()
STARTED_DOCKER=0
cleanup() {
    echo ""
    echo "Shutting down..."
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" >/dev/null 2>&1 || true
    done
    if [ "$STARTED_DOCKER" = "1" ]; then
        docker-compose down >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

if docker ps --format '{{.Names}}' | grep -q -- '-kafka-1$'; then
    echo "==> Kafka/Prometheus/Grafana already running, reusing them."
else
    echo "==> Starting Kafka, Prometheus, Grafana (docker-compose)..."
    docker-compose up -d kafka prometheus grafana
    STARTED_DOCKER=1

    echo "==> Waiting for Kafka to accept connections..."
    until docker-compose logs kafka 2>/dev/null | grep -q "Kafka Server started"; do
        sleep 1
    done
fi

echo "==> Starting backend (uvicorn on :8010)..."
(
    cd backend
    source .venv/bin/activate
    uvicorn main:app --port 8010 > /tmp/aegis-backend.log 2>&1
) &
PIDS+=($!)

echo "==> Waiting for backend to be ready..."
until curl -s http://127.0.0.1:8010/health >/dev/null 2>&1; do
    sleep 1
done

echo "==> Injecting a fault..."
curl -s -X POST http://127.0.0.1:8010/simulate/redis_leak >/dev/null

echo "==> Waiting for the incident to close (a few seconds after the trace goes quiet)..."
until [ "$(curl -s http://127.0.0.1:8010/incidents)" != "[]" ]; do
    sleep 2
done
echo "==> Root cause found:"
curl -s http://127.0.0.1:8010/incidents
echo

cat <<'EOF'

Aegis is live:
  Incidents   http://127.0.0.1:8010/incidents
  API docs    http://127.0.0.1:8010/docs
  Grafana     http://localhost:3000  (admin / aegis)
  Prometheus  http://localhost:9091

Inject more faults:
  curl -X POST http://127.0.0.1:8010/simulate/queue_backlog
  curl -X POST http://127.0.0.1:8010/simulate/deadlock_burst

Press Ctrl+C to stop everything.
EOF

wait
