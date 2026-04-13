
# setting for Kafka + state store

KAFKA_BOOTSTRAP = "kafka:9092"

RAW_TOPICS = [
    "md.orderbook.delta.futures",
    "md.orderbook.snapshot.futures"
]

OUTPUT_TOPIC = "tt.orderbook.levels.futures"

CONSUMER_GROUP = "tt-materializer"
