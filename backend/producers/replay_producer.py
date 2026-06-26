import argparse
import json
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.parser import parse_log_line

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


def load_scenario(scenario_name):
    sample_dir = os.path.join(os.path.dirname(__file__), "..", "sample_logs")
    log_path = os.path.join(sample_dir, scenario_name)
    if not os.path.exists(log_path):
        print(f"ERROR: {log_path} not found")
        sys.exit(1)
    events = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            parsed = parse_log_line(line)
            if parsed:
                events.append(parsed)
    return events


def produce_http(events, target_url, rate, jitter):
    import random
    base_delay = 1.0 / rate if rate > 0 else 0
    print(f"Producing {len(events)} events to {target_url} (rate={rate}x, jitter={jitter})")

    if jitter:
        shuffled = list(events)
        for i in range(len(shuffled) - 1, 0, -1):
            if random.random() < 0.3:
                j = random.randint(max(0, i - 3), i)
                shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
        events = shuffled

    for i, event in enumerate(events):
        safe = {k: v for k, v in event.items() if not k.startswith("_")}
        try:
            resp = requests.post(target_url, json=safe, timeout=5)
            status = resp.status_code
        except Exception as e:
            status = f"ERROR: {e}"
        print(f"  [{i+1}/{len(events)}] trace={event.get('trace_id')} service={event.get('service')} -> {status}")
        if base_delay > 0:
            time.sleep(base_delay)
    print("Done.")


def produce_kafka(events, broker, topic, rate, jitter):
    import asyncio

    async def _produce():
        from aiokafka import AIOKafkaProducer
        import random

        producer = AIOKafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await producer.start()
        print(f"Producing {len(events)} events to {broker}/{topic} (rate={rate}x, jitter={jitter})")

        send_events = list(events)
        if jitter:
            for i in range(len(send_events) - 1, 0, -1):
                if random.random() < 0.3:
                    j = random.randint(max(0, i - 3), i)
                    send_events[i], send_events[j] = send_events[j], send_events[i]

        base_delay = 1.0 / rate if rate > 0 else 0
        for i, event in enumerate(send_events):
            safe = {k: v for k, v in event.items() if not k.startswith("_")}
            trace_id = event.get("trace_id", "unknown")
            await producer.send_and_wait(topic, value=safe, key=trace_id)
            print(f"  [{i+1}/{len(send_events)}] trace={trace_id} service={event.get('service')} -> partition keyed by {trace_id}")
            if base_delay > 0:
                await asyncio.sleep(base_delay)

        await producer.flush()
        await producer.stop()
        print("Done.")

    asyncio.run(_produce())


def main():
    parser = argparse.ArgumentParser(description="Aegis Replay Producer")
    parser.add_argument("--scenario", required=True, help="Log filename (e.g. redis_retry_storm.log)")
    parser.add_argument("--sink", choices=["http", "kafka"], default="http")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--jitter", action="store_true", help="Inject out-of-order events")
    parser.add_argument("--url", default="http://127.0.0.1:8000/events", help="HTTP target URL")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--topic", default="telemetry.raw", help="Kafka topic name")
    args = parser.parse_args()

    events = load_scenario(args.scenario)

    if args.sink == "http":
        if not HAS_REQUESTS:
            print("ERROR: 'requests' library not installed. Run: pip install requests")
            sys.exit(1)
        produce_http(events, args.url, args.rate, args.jitter)
    elif args.sink == "kafka":
        produce_kafka(events, args.broker, args.topic, args.rate, args.jitter)


if __name__ == "__main__":
    main()
