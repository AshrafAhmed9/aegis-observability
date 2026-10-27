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
if docker ps --format '{{.Names}}' | grep -q -- '-kafka-1$'; then
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

echo "[3/4] Simulating an outage: the 'cache' service starts failing."
echo "      The 'api' service depends on cache, so it times out and logs far more"
echo "      errors than cache did. This is the situation that fools a person on call."
# The API can report healthy a moment before its Kafka consumer has joined,
# and the consumer ignores events sent before it joins. So if no incident
# shows up, inject again (up to 6 tries, about 30s each).
for _ in 1 2 3 4 5 6; do
    curl -s -X POST "$API/simulate/redis_leak" >/dev/null
    for _ in $(seq 1 15); do
        [ "$(curl -s "$API/incidents")" != "[]" ] && break 2
        sleep 2
    done
done
if [ "$(curl -s "$API/incidents")" = "[]" ]; then
    echo "No incident appeared. Check /tmp/aegis-backend.log"
    exit 1
fi

echo "[4/4] Aegis analyzed the events. Result:"
echo ""
# Turns the JSON answer into a short readable summary.
curl -s "$API/incidents" | python3 -c '
import json, sys
incident = json.load(sys.stdin)[0]
nodes, ranked = incident["nodes"], incident["ranked_root_causes"]
causes = {effect: cause for cause, effect in incident["edges"]}

print("  Service    Worst log level   Verdict")
print("  ---------  ----------------  ------------------------------------------")
# Root cause first, then victims, then healthy services.
order = ranked + [name for name in nodes if name not in ranked]
for service in order:
    node = nodes[service]
    if service == ranked[0]:
        verdict = "ROOT CAUSE: nothing caused this failure"
    elif service in causes:
        verdict = "victim: its errors were caused by " + causes[service]
    else:
        verdict = "healthy"
    level = node["max_severity"]
    print(f"  {service:<9}  {level:<16}  {verdict}")
print()
print(f"  How Aegis knows: every error from the victim points back at an error from")
print(f"  {ranked[0]}, so {ranked[0]} sits at the start of the chain of causes.")
'

# Shows the ML model's early-warning score for the failing service. The model
# only advises; it does not decide the root cause.
curl -s "$API/predictions" | python3 -c '
import json, sys
scores = json.load(sys.stdin)
flagged = [p for p in scores if p["high_risk"]]
if flagged:
    top = max(flagged, key=lambda p: p["risk"])
    service, risk = top["service"], top["risk"]
    print()
    print(f"  Early warning: before the failure, the ML model flagged {service} as")
    print(f"  high risk (score {risk:.2f} out of 1.0). It only advises; the graph decides.")
'

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
