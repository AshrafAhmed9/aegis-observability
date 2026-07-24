"""
Thin Kafka wrapper: send events in, get events out, as plain JSON. Nothing
else in the project needs to know Kafka is involved.
"""

import json
import logging
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = logging.getLogger(__name__)

TOPIC = "telemetry.raw"
BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")


async def send_events(events, topic=TOPIC, broker=BROKER):
    producer = AIOKafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    await producer.start()
    try:
        for event in events:
            await producer.send_and_wait(topic, event)
    finally:
        await producer.stop()


async def consume_forever(on_event, topic=TOPIC, broker=BROKER, group_id="aegis-correlators"):
    """Runs until cancelled, calling on_event(event) for every message."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        async for message in consumer:
            try:
                on_event(message.value)
            except Exception:
                # One bad message shouldn't take down the whole consumer --
                # log it and keep processing the rest of the stream.
                logger.exception("Failed to process a message; skipping it")
    finally:
        await consumer.stop()
