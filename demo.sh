#!/usr/bin/env bash
# One-command demo. Starts Kafka, Prometheus, Grafana and the Aegis API,
# simulates an outage, shows what Aegis concluded, and opens the dashboards.
# Ctrl+C stops everything.
set -euo pipefail
cd "$(dirname "$0")"

API=http://127.0.0.1:8010
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
# Ctrl+C just exits; the EXIT trap then cleans up once.
trap cleanup EXIT
trap 'exit 130' INT TERM

echo ""
echo "AEGIS DEMO"
echo "Finds the service that really caused an outage, not just the loudest one."
echo ""

echo "[1/4] Starting Kafka (event stream), Prometheus and Grafana (dashboards)..."
# Reuse Kafka only if it is really accepting connections on its port.
if (echo > /dev/tcp/127.0.0.1/9092) 2>/dev/null; then
    echo "      Already running, reusing them."
else
    docker-compose up -d kafka prometheus grafana >/dev/null 2>&1
    STARTED_DOCKER=1
    until docker-compose logs kafka 2>/dev/null | grep -q "Kafka Server started"; do
        sleep 1
    done
fi

echo "[2/4] Starting the Aegis API..."
(
    cd backend
    source .venv/bin/activate
    # exec makes uvicorn itself the background process, so Ctrl+C can stop it.
    exec uvicorn main:app --port 8010 > /tmp/aegis-backend.log 2>&1
) &
PIDS+=($!)
until curl -s "$API/health" >/dev/null 2>&1; do
    sleep 1
done

incident_count() {
    curl -s "$API/incidents" | python3 -c 'import json, sys; print(len(json.load(sys.stdin)))'
}

echo "[3/4] Causing three different outages, one after another."
echo "      In each one, a service breaks and the 'api' in front of it starts failing"
echo "      too, with more errors than the service that actually broke. That is the"
echo "      situation that fools a person on call."
echo ""
echo "      Outage 1: the cache runs out of connections (redis_leak)"
# The API can report healthy a moment before its Kafka consumer has joined,
# and the consumer ignores events sent before it joins. So if no incident
# shows up, inject again (up to 6 tries, about 30s each).
for _ in 1 2 3 4 5 6; do
    curl -s -X POST "$API/simulate/redis_leak" >/dev/null
    for _ in $(seq 1 15); do
        [ "$(incident_count)" -ge 1 ] && break 2
        sleep 2
    done
done
if [ "$(incident_count)" -lt 1 ]; then
    echo "No incident appeared. Check /tmp/aegis-backend.log"
    exit 1
fi

echo "      Outage 2: the queue's background workers crash (queue_backlog)"
curl -s -X POST "$API/simulate/queue_backlog" >/dev/null
echo "      Outage 3: the database deadlocks (deadlock_burst)"
curl -s -X POST "$API/simulate/deadlock_burst" >/dev/null

echo "      Waiting for Aegis to finish analyzing them (a few seconds each)..."
for _ in $(seq 1 40); do
    [ "$(incident_count)" -ge 3 ] && break
    sleep 2
done
if [ "$(incident_count)" -lt 3 ]; then
    echo "Only $(incident_count) of 3 incidents appeared. Check /tmp/aegis-backend.log"
    exit 1
fi

echo "[4/4] Results. Aegis is never told which service we broke."
echo ""
# Turns the JSON answers into a short readable summary for each outage.
INCIDENTS="$(curl -s "$API/incidents")" PREDICTIONS="$(curl -s "$API/predictions")" python3 - <<'PY'
import json
import os

incidents = json.loads(os.environ["INCIDENTS"])
predictions = json.loads(os.environ["PREDICTIONS"])

# What we injected, in order: (story, the service we broke).
OUTAGES = [
    ("The cache runs out of connections", "cache"),
    ("The queue's background workers crash", "queue"),
    ("The database deadlocks", "database"),
]

# Each outage breaks a different service, so the root cause identifies it.
incident_for = {item["ranked_root_causes"][0]: item for item in incidents}

correct = 0
for number, (story, broken) in enumerate(OUTAGES, start=1):
    incident = incident_for.get(broken)
    print(f"  OUTAGE {number} of 3: {story}")
    if incident is None:
        print("    Aegis found no root cause for this outage.\n")
        continue

    nodes, ranked = incident["nodes"], incident["ranked_root_causes"]
    caused_by = {effect: cause for cause, effect in incident["edges"]}

    print("    Service    Worst log level   Verdict")
    print("    ---------  ----------------  -----------------------------------------")
    # Root cause first, then victims, then healthy services.
    for service in ranked + [name for name in nodes if name not in ranked]:
        level = nodes[service]["max_severity"]
        if service == ranked[0]:
            verdict = "ROOT CAUSE: nothing caused this failure"
        elif service in caused_by:
            verdict = "victim: its errors were caused by " + caused_by[service]
        else:
            verdict = "healthy"
        print(f"    {service:<9}  {level:<16}  {verdict}")

    blamed = ranked[0]
    right = blamed == broken
    correct += right
    print(f"    Aegis blamed {blamed}. We broke {broken}. {'Correct.' if right else 'WRONG.'}")

    flagged = [p["risk"] for p in predictions if p["service"] == broken and p["high_risk"]]
    if flagged:
        print(f"    Early warning: the ML model flagged {broken} as high risk at its first")
        print(f"    WARNING, before the failure (score {max(flagged):.2f} of 1.0). It only advises.")
    print()

print(f"  {correct} of 3 outages traced to the right service.")
print("  How: each victim's errors point back at an error from the service that broke,")
print("  so the service with nothing pointing at it is the root cause.")
PY

echo ""
echo "Opening the dashboards in your browser:"
echo "  Grafana  - live count of events and incidents Aegis has processed"
echo "  API docs - click POST /simulate/{fault_name} > Try it out to cause another outage"
sleep 2
open "http://localhost:3000/d/aegis/aegis?refresh=5s" 2>/dev/null || xdg-open "http://localhost:3000/d/aegis/aegis?refresh=5s" 2>/dev/null || true
open "$API/docs" 2>/dev/null || xdg-open "$API/docs" 2>/dev/null || true

cat <<'EOF'

If the pages do not open, copy these into your browser:
  Grafana     http://localhost:3000/d/aegis/aegis
  API docs    http://127.0.0.1:8010/docs
  Prometheus  http://localhost:9091   (raw metrics, mostly for debugging)

Cause another outage from the terminal (fault names: redis_leak, queue_backlog, deadlock_burst):
  curl -X POST http://127.0.0.1:8010/simulate/deadlock_burst
  curl http://127.0.0.1:8010/incidents      # a few seconds later

Press Ctrl+C to stop everything.
EOF

wait
