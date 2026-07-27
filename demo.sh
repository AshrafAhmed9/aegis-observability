#!/usr/bin/env bash
# One-command interview demo: brings up Kafka/Prometheus/Grafana, the API,
# and the frontend, then injects a fault so there's something on screen
# immediately. Ctrl+C tears everything down.
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

echo "==> Starting frontend (vite on :5173)..."
(
    cd frontend
    npm run dev > /tmp/aegis-frontend.log 2>&1
) &
PIDS+=($!)

echo "==> Waiting for backend to be ready..."
until curl -s http://127.0.0.1:8010/health >/dev/null 2>&1; do
    sleep 1
done

echo "==> Injecting a fault so an incident is already on screen..."
curl -s -X POST http://127.0.0.1:8010/simulate/redis_leak >/dev/null

echo "==> Opening the console..."
sleep 2
open http://localhost:5173 2>/dev/null || xdg-open http://localhost:5173 2>/dev/null || true

cat <<'EOF'

Aegis is live:
  Frontend    http://localhost:5173
  API docs    http://127.0.0.1:8010/docs
  Grafana     http://localhost:3000  (admin / aegis)
  Prometheus  http://localhost:9091

Inject more faults from the console buttons, or:
  curl -X POST http://127.0.0.1:8010/simulate/queue_backlog
  curl -X POST http://127.0.0.1:8010/simulate/deadlock_burst

Press Ctrl+C to stop everything.
EOF

wait
