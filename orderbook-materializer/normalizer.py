# raw -> normalized event

"""
Event Normalization Layer

This function converts raw Kafka order book events into a
standardized internal structure used by the SCD2 engine.

The producer may send exchange-specific fields, but the SCD2
materializer requires a consistent schema across all exchanges.

The normalized structure contains:

- symbol: Trading pair identifier
- seq: Sequence number for ordering and deduplication
- type: "snapshot" or "delta"
- bids: List of (price, quantity) updates
- asks: List of (price, quantity) updates
- event_time: Exchange event timestamp (Unix millis)

Why this step is necessary:

- Decouples exchange-specific payload formats from core logic
- Ensures deterministic SCD2 processing
- Simplifies testing and replay
- Enables multi-exchange compatibility

This layer must remain lightweight and stateless.
"""

def normalize(event):
    return {
        "symbol": event["symbol"],
        "seq": event["seq"],
        "type": event["type"],
        "bids": event["bids"],
        "asks": event["asks"],
        "event_time": event["event_time"]
    }
