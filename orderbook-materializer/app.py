  # main consumer loop

"""
Main Materializer Application

Consumes raw order book events from Kafka,
applies real-time SCD2 transformation,
and publishes generated SCD2 rows to the output topic.

Responsible for:
- Event ordering and deduplication
- State management per symbol
- Emitting close/open SCD2 records

This is the core real-time processing loop of the system.
"""


from confluent_kafka import Consumer, Producer
import json
from config import *
from state_store import SymbolState
from scd2_engine import apply_event
from dedupe import should_process
from normalizer import normalize

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest"
})

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP
})

consumer.subscribe(RAW_TOPICS)

states = {}

while True:
    msg = consumer.poll(1.0)
    if not msg:
        continue

    key = msg.key().decode()
    event = json.loads(msg.value())

    if key not in states:
        states[key] = SymbolState()

    state = states[key]

    if not should_process(state, event["seq"]):
        continue

    event = normalize(event)
    rows = apply_event(state, event)

    for row in rows:
        producer.produce(
            OUTPUT_TOPIC,
            key=key,
            value=json.dumps(row)
        )
