# Architecture

## Overview

This project is a layered market data pipeline for ingesting real-time order book depth updates from multiple exchanges, transporting them through Kafka, transforming them into a canonical format, generating SCD history, and persisting the results in ClickHouse.

The current application code lives under `data/`.

## High-Level Flow

```text
Exchange WebSockets
    -> Producer / Collectors
    -> Kafka Raw Topics
    -> Normalization Processor
    -> Kafka Normalized Topic
    -> SCD Processor
    -> Kafka SCD Topic
    -> ClickHouse
```

## System Diagram

```text
                        +-------------------------------+
                        |      Exchange WebSockets      |
                        | Binance | Bybit | OKX | ...   |
                        +---------------+---------------+
                                        |
                                        | real-time depth events
                                        v
                        +-------------------------------+
                        |     Producer / Collectors     |
                        | one collector per exchange,   |
                        | market, and symbol            |
                        +---------------+---------------+
                                        |
                                        | publish raw events
                                        v
                        +-------------------------------+
                        |             Kafka             |
                        | transport, buffering, replay  |
                        +---------------+---------------+
                                        |
                        +---------------+---------------+
                        |                               |
                        | consume raw                   | consume normalized
                        v                               v
            +---------------------------+    +---------------------------+
            |   Normalization Layer     |    |       SCD Layer          |
            | exchange-specific payload |    | price level history      |
            | -> canonical order book   |    | open/close/current rows  |
            +-------------+-------------+    +-------------+-------------+
                          |                                |
                          | publish normalized             | publish/write SCD
                          v                                v
                 +-------------------+            +-----------------------+
                 | Kafka Normalized  |            |     ClickHouse        |
                 | md.norm.depth.v1  |            | raw + normalized +    |
                 +-------------------+            | scd persistence       |
                                                  +-----------------------+
```

## Logical Layers

### 1. Raw Layer

The raw layer preserves the original exchange event with minimal transformation.

Responsibilities:

- Maintain WebSocket connections to each exchange.
- Subscribe to exchange-specific order book streams.
- Attach ingestion metadata such as `exchange`, `market`, `symbol`, and `ingest_time_ms`.
- Publish the source event to Kafka without losing exchange-specific fields.

Typical topic naming:

```text
md.raw.<exchange>.<market>.depth.v1
```

Examples:

```text
md.raw.binance.futures.depth.v1
md.raw.bybit.spot.depth.v1
md.raw.okx.swap.depth.v1
```

### 2. Normalization Layer

The normalization layer converts exchange-specific payloads into a shared canonical schema.

Responsibilities:

- Parse each raw exchange event.
- Normalize field names and event structure.
- Standardize bids, asks, timestamps, event type, and sequence values.
- Validate event shape before downstream processing.
- Publish canonical events to a single normalized topic.

Typical topic naming:

```text
md.norm.depth.v1
```

 canonical fields:

```text
exchange
market
symbol
event_type
event_time_ms
ingest_time_ms
sequence
prev_sequence
bids
asks
extensions
```

`extensions` should be used for exchange-specific fields that do not belong in the shared schema but still need to be preserved.

### 3. SCD Layer

The SCD layer transforms normalized order book events into historical price-level state changes.

Responsibilities:

- Maintain per-stream order book state.
- Apply snapshot and delta updates.
- Detect level inserts, updates, and removals.
- Emit Slowly Changing Dimension rows for each level lifecycle.
- Track `valid_from`, `valid_to`, and `is_current`.

Typical topic naming:

```text
md.scd.depth.levels.v1
```

Suggested SCD fields:

```text
exchange
market
symbol
side
price
qty
valid_from
valid_to
is_current
source_event_time_ms
source_sequence
```

## Runtime Components

### Collectors

Collectors are exchange-specific ingestion components. A collector is created per `exchange + market + symbol`.

Examples:

- `binance.futures.BTCUSDT`
- `binance.futures.ETHUSDT`
- `bybit.spot.BTCUSDT`

This design keeps exchange-specific parsing isolated and makes per-symbol failures easier to observe and recover from.

### Exchange Stream Semantics

Order book streams are not identical across exchanges. In general, exchanges follow one of these patterns:

- Snapshot + delta: a full order book snapshot establishes the initial state and later updates modify that state incrementally.
- Delta-only: only changes are streamed, so the consumer must already have a valid starting state from another source.
- WebSocket snapshot + WebSocket updates: the stream itself sends the initial snapshot and later updates on the same connection.

This difference matters because correctness depends on:

- Starting from a valid snapshot or baseline.
- Applying updates in the correct order.
- Detecting gaps, stale streams, or resets.
- Forcing a resync when continuity is broken.

#### Binance

Binance uses a `REST snapshot + WebSocket delta` model.

General behavior:

- Subscribe to the WebSocket depth stream.
- Buffer incoming deltas.
- Fetch a REST order book snapshot.
- Find the first buffered delta that bridges the snapshot.
- Apply later deltas in sequence.

Important sequencing fields:

- `lastUpdateId` from the REST snapshot
- `U` = first update ID in a WebSocket event
- `u` = final update ID in a WebSocket event
- `pu` may also be present on some streams as the previous final update ID

Meaning:

- Binance requires explicit synchronization logic between REST and WebSocket.
- If sequence continuity is broken, the local book should be rebuilt from a new snapshot.

#### Bybit

Bybit generally uses a `WebSocket snapshot + WebSocket delta` model.

General behavior:

- Subscribe to the order book topic.
- Receive a snapshot message first.
- Receive later delta messages afterward.
- Rebuild local state from the snapshot and then apply later updates in order.

Typical fields:

- `type` often indicates `snapshot` or `delta`
- `u` or `seq` may represent update ordering
- `cts` or stream timestamps may be included

Meaning:

- Bybit does not usually require a separate REST snapshot for normal stream bootstrap.
- The collector should treat the first snapshot as the baseline.
- If continuity is lost or the stream becomes stale, the safest action is to resubscribe and rebuild from the next snapshot.

#### OKX

OKX `books` channel uses a `WebSocket snapshot + WebSocket update` model.

General behavior:

- Subscribe to the `books` channel.
- Receive an initial full snapshot on the WebSocket.
- Receive later incremental updates on the same WebSocket connection.

Important sequencing fields:

- `action = "snapshot"` for the full snapshot
- `action = "update"` for incremental changes
- `seqId` = current sequence
- `prevSeqId` = previous sequence

Meaning:

- OKX does not need the Binance-style REST bootstrap for the `books` channel.
- Synchronization depends on correctly handling `snapshot` vs `update`.
- Continuity is checked using `prevSeqId -> seqId`.

### Data Frequency / Request Timing

This pipeline mainly uses WebSocket subscriptions, so most raw market data is not pulled on a fixed polling interval. Exchanges push updates whenever the subscribed stream emits data.

#### Binance

Binance update frequency is controlled by `depth_stream` in configuration.

Example:

```yaml
depth_stream: "depth@100ms"
```

Meaning:

- `depth@100ms` requests depth updates roughly every `100 ms`.
- This is the main frequency control for Binance spot and futures streams.

Notes:

- This value is used in the Binance WebSocket subscription string.
- A REST snapshot is fetched only once during bootstrap or reconnect, not continuously.

#### Bybit

Bybit uses channel subscription names such as:

```yaml
channel: "orderbook.50"
```

Meaning:

- This selects the Bybit order book stream.
- Update timing is determined by the exchange stream, not by a local polling timer.

#### OKX

OKX uses channels such as:

```yaml
channel: "books"
```

Meaning:

- This selects the OKX order book stream.
- Updates are pushed by OKX after subscription.
- There is no fixed request interval in local code for ongoing updates.

#### Reconnect Timing

Reconnect timing is controlled by:

```yaml
pipeline:
  reconnect_base_seconds: 1
  reconnect_max_seconds: 30
```

Meaning:

- On connection failure, collectors retry with backoff.
- Retry starts around `1 second`.
- Retry grows up to `30 seconds`.

#### Kafka Producer Batching

Kafka producer batching is controlled by:

```yaml
kafka:
  linger_ms: 5
```

Meaning:

- The producer may wait up to `5 ms` before sending a batch.
- This is a batching and throughput setting, not market data request frequency.

#### Summary

- Binance has an explicit stream cadence such as `depth@100ms`.
- Bybit and OKX are subscription-driven and exchange-controlled.
- Reconnect timing is local and configured in `pipeline`.
- Kafka `linger_ms` affects send batching, not exchange polling frequency.

### Kafka

Kafka is the transport backbone of the system.

Key features in this project:

- Decoupling between collectors and downstream processors.
- Buffering for bursty market data.
- Per-stream ordering with keys such as `exchange|market|symbol`.
- Replay from offsets for recovery or reprocessing.
- Lets different consumers read the same topic for different purposes..
- Scalable consumption with consumer groups.
- Support for idempotent production and deduplication-aware processing.

Recommended partition key:

```text
exchange|market|symbol
```

This keeps event ordering stable at the stream level.

### Normalization and SCD Example

One raw event can contain multiple price levels:

```json
{
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "event_type": "delta",
  "sequence": 1001,
  "prev_sequence": 1000,
  "bids": [["68000.1", "1.2"], ["67999.8", "0"]],
  "asks": [["68001.2", "0.7"], ["68002.0", "2.1"]]
}
```

After normalization, it is still one canonical event:

```json
{
  "exchange": "binance",
  "market": "futures",
  "symbol": "BTCUSDT",
  "event_type": "delta",
  "event_time_ms": 1710000000000,
  "ingest_time_ms": 1710000000100,
  "sequence": 1001,
  "prev_sequence": 1000,
  "bids": [["68000.1", "1.2"], ["67999.8", "0"]],
  "asks": [["68001.2", "0.7"], ["68002.0", "2.1"]]
}
```

After SCD processing, that one event can produce multiple history rows, one per affected level:

```text
exchange market  symbol  side price    qty valid_from valid_to is_current
binance futures BTCUSDT bid  68000.1  1.2 1001       null     1
binance futures BTCUSDT bid  67999.8  0   1001       1001     0
binance futures BTCUSDT ask  68001.2  0.7 1001       null     1
binance futures BTCUSDT ask  68002.0  2.1 1001       null     1
```

This is the main distinction between the layers:

- Raw layer: one exchange-native event.
- Normalization layer: one canonical event.
- SCD layer: many rows from that event, one per changed price level.

### Processor / Pipeline

The processing stage consumes Kafka events and performs the business transformation.

Responsibilities:

- Consume raw events.
- Normalize payloads into canonical form.
- Maintain order book state where required.
- Generate SCD records.
- Publish normalized and SCD events or write them directly to storage.

### ClickHouse

ClickHouse is the analytical storage layer.

  tables:

- `marketdata.orderbook_events_raw`
- `marketdata.orderbook_levels_scd`

Recommended design principles:

- Use one table per logical layer, not one table per exchange.
- Keep `exchange`, `market`, and `symbol` as dimensions in every table.
- Partition primarily by time, optionally with exchange as part of the key.

Example partition strategy:

```text
PARTITION BY (toYYYYMM(event_time), exchange)
```

##  Project Structure

The  implementation is under `data/`.

```text
data/
|-- collectors/
|-- config/
|-- processors/
|-- producers/
|-- orderbook-processor/
|-- orderbook-producer/
|-- main.py
|-- Dockerfile
|-- docker-compose.yml
`-- requirements.txt
```

Core responsibilities by folder:

- `data/collectors/`: exchange-specific WebSocket ingestion.
- `data/producers/`: Kafka publishing and output routing.
- `data/processors/`: normalization and SCD logic.
- `data/config/`: YAML configuration and topic declarations.
- `data/orderbook-processor/`: downstream materialization/persistence logic.

## Kafka Topics

The pipeline uses three logical Kafka topic layers.

### Raw Topics

Pattern:

```text
md.raw.<exchange>.<market>.depth.v1
```

Purpose:

Carries exchange-native order book events exactly as they are received from each source, with minimal enrichment such as `exchange`, `market`, `symbol`, and ingest metadata.

Examples:

```text
md.raw.binance.spot.depth.v1
md.raw.binance.futures.depth.v1
md.raw.bybit.spot.depth.v1
md.raw.bybit.linear.depth.v1
md.raw.okx.margin.depth.v1
md.raw.okx.swap.depth.v1
```

### Normalized Topic

Topic:

```text
md.norm.depth.v1
```

Purpose:

Carries canonical order book events after exchange-specific payloads have been normalized into a shared schema.

### SCD Topic

Topic:

```text
md.scd.depth.levels.v1
```

Purpose:

Carries SCD-style price level history events generated from normalized order book updates.

## Design Principles

### Preserve Raw Truth

Raw events should remain as close as possible to the exchange payload so that replay and debugging are always possible.

### Normalize Once

Canonical transformation should happen in one place so downstream consumers do not need exchange-specific logic.

### Build History from Canonical State

SCD generation should use normalized events and a shared book-state model instead of re-implementing logic per exchange.

### Separate Transport from Processing

Kafka should isolate ingestion from downstream persistence and analytics workloads.

## Operational Notes

### Exchange-Specific Bootstrap

Some exchanges require a snapshot bootstrap before deltas can be safely applied. This should be treated as part of the collector or synchronization logic, not part of the raw Kafka transport layer.

### Failure Isolation

Per-symbol collectors isolate failures, but they also increase connection count. This is simple and observable, though a multiplexed-per-market connection model may be preferable later for scale.

### Data Replay

Because raw events are stored and transported first, normalization and SCD logic can be replayed if the schema or business rules change.

## Summary

This project is best understood as a three-layer data platform:

```text
Raw ingestion -> Canonical normalization -> SCD history generation
```

Kafka is the transport spine between stages, and ClickHouse is the persistence layer for queryable history and analytics.

