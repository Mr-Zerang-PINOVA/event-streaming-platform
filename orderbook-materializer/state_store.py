"""
State Store Module

This module maintains the in-memory state required for real-time SCD2
order book materialization.

For each (exchange|market|symbol), it stores:

- last_seq: The last processed sequence number (used for deduplication).
- bids: Active bid levels (price -> {qty, valid_from}).
- asks: Active ask levels (price -> {qty, valid_from}).

This state enables:

- Idempotent event processing (no duplicate rows).
- Correct SCD2 close/open interval generation.
- Deterministic real-time order book reconstruction.

In production environments, this in-memory store should be replaced
or backed by a persistent state store such as:

- RocksDB
- Kafka Streams State Store

to ensure fault tolerance, durability, and safe recovery after restarts.
"""
class SymbolState:
    def __init__(self):
        self.last_seq = 0
        self.bids = {}  # price -> {qty, valid_from}
        self.asks = {}
