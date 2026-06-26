import asyncio
import json
import os
import sys
import time
from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.streaming import StreamingCorrelator, IncidentAssembler
from app.correlation import CorrelationEngine
from app.analyzer import AegisAnalyzer
from app.jetro_service import JetroService
from app.metrics import (
    EVENTS_INGESTED, TRACES_EMITTED, INCIDENTS_PROCESSED,
    LATE_EVENTS, OPEN_TRACES, CORRELATION_DURATION, ROOT_CAUSE_CLASS,
)

WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry.raw")
GROUP_ID = os.environ.get("KAFKA_GROUP", "aegis-correlators")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9090"))
TICK_INTERVAL = 2.0

async def run_consumer():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print(f"Consumer started: broker={BROKER} topic={TOPIC} group={GROUP_ID}")

    correlator = StreamingCorrelator(grace=30.0)
    assembler = IncidentAssembler()

    try:
        while True:
            batch = await consumer.getmany(timeout_ms=int(TICK_INTERVAL * 1000))
            event_count = 0
            all_closed = []

            for tp, messages in batch.items():
                for msg in messages:
                    event = msg.value
                    closed = correlator.ingest(event)
                    all_closed.extend(closed)
                    event_count += 1

            closed_from_tick = correlator.tick()
            all_closed.extend(closed_from_tick)

            EVENTS_INGESTED.inc(event_count)
            TRACES_EMITTED.inc(len(all_closed))
            LATE_EVENTS.inc(correlator.late_count)
            OPEN_TRACES.set(correlator.open_trace_count)

            for trace in all_closed:
                incident_events = assembler.add(trace)
                if incident_events:
                    process_incident(incident_events)

            if event_count > 0:
                print(f"Processed {event_count} events, {len(all_closed)} traces closed, "
                      f"{correlator.open_trace_count} open, {correlator.late_count} late")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        remaining_traces = correlator.flush_all()
        for trace in remaining_traces:
            incident_events = assembler.add(trace)
            if incident_events:
                process_incident(incident_events)
        final = assembler.flush()
        if final:
            process_incident(final)

        await consumer.stop()
        print("Consumer stopped.")

def process_incident(incident_events):
    start = time.monotonic()

    nodes, edges = CorrelationEngine.build_propagation_graph(incident_events)
    blast_radius = CorrelationEngine.estimate_blast_radius(incident_events, nodes)
    rca_result = CorrelationEngine.classify_root_cause(nodes, edges)
    report = AegisAnalyzer.analyze("kafka-stream", incident_events, blast_radius, rca_result)

    jetro = JetroService(WORKSPACE_ROOT)
    jetro.export_all(report, incident_events, nodes, edges)

    duration = time.monotonic() - start
    CORRELATION_DURATION.observe(duration)
    INCIDENTS_PROCESSED.inc()

    top_root = rca_result["ranked_root_causes"][0] if rca_result["ranked_root_causes"] else None
    if top_root:
        ROOT_CAUSE_CLASS.labels(root_cause_class=top_root["root_cause_class"]).inc()

    root_info = f"{top_root['service']} ({top_root['root_cause_class']})" if top_root else "unknown"
    print(f"  INCIDENT: {report.incident_id} | root={root_info} | "
          f"severity={report.overall_severity} | events={len(incident_events)} | "
          f"duration={duration:.3f}s")

def main():
    start_http_server(METRICS_PORT)
    print(f"Prometheus metrics serving on :{METRICS_PORT}/metrics")

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(run_consumer())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == "__main__":
    main()
