# Event Streaming Platform SCD Runbook

This runbook helps you validate end-to-end order book SCD behavior across:
- Producer (WebSocket ingestion)
- Kafka (stream transport)
- Processor/Pipeline (normalization and SCD generation)
- ClickHouse (raw and SCD persistence)

## Prerequisites

- Docker and Docker Compose are installed.
- You are in the project root:

```bash
cd /mnt/c/sources/gitlabsource/data/event-streaming-platform
```

- Python venv is optional unless you run local Python commands:

```bash
python -m venv .venv
source .venv/bin/activate
```

## Quick Start

Build and start services:

```bash
docker compose up -d --build
docker compose ps
```

## Core Log Checks

### Producer

```bash
docker compose logs pipeline --tail=200
docker compose logs pipeline --tail=500 | grep -E "ws connected|ws error|ws closed|ws stale|ws loop exited|reconnecting|heartbeat|waiting for WebSocket|GAP detected|RESYNC requested|aligned using buffer"
```

### Processor

```bash
docker compose logs processor --tail=500
docker compose logs processor --tail=500 | grep -E "progress consumed_events|wrote closed_rows|aligned on delta|GAP:|No bridging delta"
```

### Pipeline

```bash
docker compose logs pipeline --tail=200 | grep -Ei "Pipeline started|Connecting|Connected|Subscribed|error|reconnect|futures.BTCUSDT|futures.ETHUSDT"
```

If ETH stream is expected, verify it is enabled in Docker config:

```bash
sed -n '30,50p' config/config.docker.yaml
docker exec event-streaming-platform-pipeline-1 sh -lc "sed -n '30,50p' /app/config/config.docker.yaml"
```

Rebuild pipeline only:

```bash
docker compose build --no-cache pipeline
docker compose up -d --force-recreate --no-deps pipeline
```

## Kafka Checks

List topics:

```bash
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --list"
```

`__consumer_offsets` is a Kafka internal topic and is expected.

Read sample messages:

```bash
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic md.norm.depth.v1 --max-messages 5 --timeout-ms 10000"
```

Count `snapshot` vs `delta` from sampled history:

```bash
docker exec -it kafka bash -lc '
kafka-console-consumer --bootstrap-server kafka:9092 --topic md.norm.depth.v1 --from-beginning --max-messages 2000 \
| grep -oE "\"event_type\"[[:space:]]*:[[:space:]]*\"(snapshot|delta)\"" \
| sed -E "s/.*\"(snapshot|delta)\"/\1/" \
| sort | uniq -c
'
```

Check consumer lag and latest topic offset:

```bash
# 1) Show processor consumer-group lag (CURRENT-OFFSET, LOG-END-OFFSET, LAG)
docker exec -it kafka bash -lc "kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group md.dev.sink.clickhouse.scd.v1"

# 2) Show latest committed partition offsets for the normalized topic (time -1 = latest)
docker exec -it kafka bash -lc "kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic md.norm.depth.v1 --time -1"
```

## ClickHouse Checks

For reusable SQL checks, see:

- `docs/clickhouse/validation_queries.sql`
- `docs/clickhouse/README.md`

Show tables and row counts:

```bash
docker exec -it clickhouse clickhouse-client -q "SHOW TABLES FROM marketdata"
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM marketdata.orderbook_events_raw"
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM marketdata.orderbook_levels_scd"
```

Recent raw events:

```bash
docker exec -it clickhouse clickhouse-client -q "SELECT event_time_ms,event_type,update_id_from,update_id_to,snapshot_last_update_id FROM marketdata.orderbook_events_raw ORDER BY event_time_ms DESC LIMIT 20"
```

Recent SCD rows:

```bash
docker exec -it clickhouse clickhouse-client -q "SELECT exchange,market,symbol,side,price,qty,valid_from,valid_to,opened_by,closed_by,update_id_from,update_id_to FROM marketdata.orderbook_levels_scd ORDER BY valid_from DESC LIMIT 10"
```

JSON array flattening example:

```sql
SELECT
  arrayJoin(JSONExtractArrayRaw(bids_json)) AS level,
  JSONExtractString(level, 1) AS price,
  JSONExtractString(level, 2) AS qty
FROM marketdata.orderbook_events_raw
LIMIT 10;
```

## SCD Integrity Validation
docker exec -it clickhouse clickhouse-client -u clickhouse --password clickhouse -d marketdata

Check global duplicates:

```sql
SELECT
  count() AS total_rows,
  uniqExact(exchange, market, symbol, side, price, valid_from, qty) AS unique_rows
FROM marketdata.orderbook_levels_scd;
```

Open-row uniqueness check:

```sql
SELECT
  exchange, market, symbol, side, price,
  count() AS open_rows
FROM marketdata.orderbook_levels_scd
WHERE valid_to IS NULL
GROUP BY exchange, market, symbol, side, price
HAVING open_rows > 1
ORDER BY open_rows DESC, symbol, side, price;
```

Closed-row duplicate check:

```sql
SELECT
  exchange, market, symbol, side, price,
  valid_from, valid_to,
  count() AS dup_rows
FROM marketdata.orderbook_levels_scd
WHERE valid_to IS NOT NULL
GROUP BY exchange, market, symbol, side, price, valid_from, valid_to
HAVING dup_rows > 1
ORDER BY dup_rows DESC, symbol, side, price, valid_from;
```

## Disconnect/Reconnect Incident Checks

Use this section when producer WebSocket disconnects and reconnects.

### 1) Verify reconnect and resync sequence

```bash
 
docker compose logs -f pipeline | grep -E "Connected|Reconnecting|connect_timeout|dns_resolution_failed|network_unreachable|Fetched bootstrap snapshot"
```

Expected:
- Disconnect markers: `ws error`, `ws closed`, `ws loop exited`, `ws stale`
- Recovery markers: `GAP detected`, `RESYNC requested`, `aligned using buffer`

### 2) Count incidents in a time window

```bash
docker compose logs producer --since=30m | grep -c "GAP detected"
docker compose logs producer --since=30m | grep -c "RESYNC requested"
docker compose logs producer --since=30m | grep -c "aligned using buffer"
```

### 3) Confirm snapshots persisted

```bash
docker exec -i clickhouse clickhouse-client -q "SELECT event_time_ms,event_type,update_id_from,update_id_to,snapshot_last_update_id FROM marketdata.orderbook_events_raw WHERE event_type='snapshot' ORDER BY event_time_ms DESC LIMIT 20"
```

### 4) Confirm processor progression

```bash
docker compose logs processor --tail=400 | grep -E "GAP:|No bridging delta|aligned on delta|wrote closed_rows|progress consumed_events"
```

### 5) Outcome

- Recovered: reconnect succeeded and processor keeps progressing.
- Incident: repeated gap/resync loops without stable progress.

## Lag and Catch-Up Validation

### 1) Capture lag baseline

```bash
docker exec -it kafka bash -lc "kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe"
docker exec -it kafka bash -lc "kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic md.norm.depth.v1 --time -1"
```

Track:
- `CURRENT-OFFSET`
- `LOG-END-OFFSET`
- `LAG`

### 2) Confirm reconnect sequence

```bash
docker compose logs producer --since=30m | grep -E "ws error|Network is unreachable|ws closed|ws loop exited|ws stale|ws connected|GAP detected|RESYNC requested|aligned using buffer"
```

### 3) Track lag trend

```bash
watch -n 5 "docker exec -i kafka bash -lc 'kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group md.dev.sink.clickhouse.scd.v1'"
```

If `watch` is unavailable:

```bash
docker exec -it kafka bash -lc "kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group md.dev.sink.clickhouse.scd.v1"
```

Healthy trend:
- `CURRENT-OFFSET` increases.
- `LAG` decreases toward normal.

### 4) Confirm processor consumption

```bash
docker compose logs processor --since=30m | grep -E "progress consumed_events|wrote closed_rows|aligned on delta|GAP:|No bridging delta"
```

### 5) Decision matrix

- Healthy recovery: reconnect succeeded, processor progresses, lag stabilizes low.
- Catch-up in progress: lag is high but consistently decreasing.
- Stuck incident: lag stays flat/increases or processor shows no progress.

## Run Local Pipeline (Optional)

Use this mode if you run `main.py` directly after containers are up.

```bash
python main.py --config config/config.yaml
```

Verify topic flow:

```bash
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --list"
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic md.raw.binance.futures.depth.v1 --from-beginning --max-messages 3 --property print.key=true"
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic md.norm.depth.v1 --from-beginning --max-messages 3 --property print.key=true"
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic md.scd.depth.levels.v1 --from-beginning --max-messages 3 --property print.key=true"
```

Check persistence:

```bash
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM marketdata.orderbook_events_raw"
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM marketdata.orderbook_levels_scd"
```

## Live Monitoring (3 Terminals)

Check md topics:

```bash
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --list | grep '^md\.'"
```

Monitor normalized topic:

```bash
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic md.norm.depth.v1 --property print.key=true --property key.separator=' | '"
```

Monitor SCD topic:

```bash
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic md.scd.depth.levels.v1 --property print.key=true --property key.separator=' | '"
```

Monitor consumer lag:

```bash
docker exec -it kafka bash -lc "kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe"
```

Enable debug logging during troubleshooting (`config/config.yaml`):

```yaml
pipeline:
  log_level: "DEBUG"
```

## Reset and Cleanup

Full restart while keeping volumes:

```bash
docker compose down
docker compose up -d --build
```

Reset ClickHouse and Kafka volumes:

```bash
docker compose down
docker volume rm event-streaming-platform_clickhouse-data
docker volume rm event-streaming-platform_kafka-data
docker compose up -d
python main.py --config config/config.yaml

```


## CloudBeaver

Start CloudBeaver:

```bash
docker compose up -d dbeaver
```

Open `http://localhost:8978` and connect with:
- Host: `clickhouse`
- Port: `8123`
- Database: `marketdata`
- User: `clickhouse`
- Password: `clickhouse`

# 1) show only gap events (with timestamps)
docker compose logs -t processor --since=30m | grep -E "GAP:|No bridging delta"


# 2) count gaps per stream
docker compose logs -t processor --since=30m \
| grep -E "GAP:|No bridging delta" \
| sed -nE 's/.*stream=([^ ]+).*/\1/p' \
| sort | uniq -c


# 3) live watch (new gaps only)
docker compose logs -f processor | grep -E "GAP:|No bridging delta|aligned on delta"


docker compose logs pipeline --since=30m | grep -E "Collector error|Reconnecting|Fetched bootstrap snapshot|Failed to fetch bootstrap snapshot"


docker compose restart pipeline
docker compose logs pipeline --since=5m | grep -E "Fetched bootstrap snapshot|Publishing bootstrap updates"
