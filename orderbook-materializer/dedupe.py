    # last_seq + ttl hash cache
"""
Duplicate Event Protection

This logic prevents reprocessing of duplicate or out-of-order events.

Each symbol maintains its own `last_seq` (last processed sequence number).
An incoming event is processed only if:

    event.seq > state.last_seq

If the sequence number is less than or equal to the stored value,
the event is ignored.

This guarantees:

- Idempotent processing (safe against Kafka re-delivery)
- Protection against replayed events
- No duplicate SCD2 rows
- Deterministic state evolution

This mechanism assumes that the exchange provides strictly
increasing sequence numbers per symbol.
"""


def should_process(state, seq):
    if seq <= state.last_seq:
        return False
    state.last_seq = seq
    return True
