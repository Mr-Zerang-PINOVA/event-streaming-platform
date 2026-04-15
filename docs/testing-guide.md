# Testing Guide (EC2 + Docker + Kafka)

This guide covers practical checks for this project: service health, exchange connectivity, end-to-end data flow, sequence-gap behavior, and Kafka lag/queue monitoring.

## 1) Service Health

Run from project root:

```bash
cd /home/ubuntu/event-streaming-platform
systemctl status docker
docker compose ps
```

Expected:
- `kafka` is `healthy`
- `pipeline` and `processor` are `Up`
- `init-topics` may be `Exited` (one-time init)

### 1.1 Host memory check

Use this before deeper troubleshooting if containers are restarting, lag is growing, or the host feels pressured.

```bash
free -h
cat /proc/meminfo | egrep 'MemAvailable|Cached|Buffers'
```

Expected:
- `MemAvailable` is not critically low
- `Cached` and `Buffers` are present, which is normal on Linux
- If available memory is consistently tight, Docker services may slow down or restart under pressure

### 1.2 Pipeline WebSocket buffering settings

These settings affect how much incoming market-data traffic can be buffered in memory before the collector processes it.

- `websocket_max_queue`
  - Maximum number of incoming WebSocket messages buffered internally by the `websockets` client per connection.
  - Higher values tolerate short bursts better but increase memory usage.
  - Lower values reduce memory usage and apply backpressure sooner.

- `websocket_max_size_bytes`
  - Maximum allowed size in bytes for a single incoming WebSocket message.
  - Protects the process from unexpectedly large messages consuming excessive memory.
  - If set too low, valid exchange messages may be rejected.

- `WebSocket queue`
  - This means the `websockets` library's internal receive queue, not the pipeline's per-stream output queue.
  - Incoming messages wait here before the collector reads them in the `async for raw_message in websocket_client` loop.

Where configured:

```yaml
exchanges:
  binance:
    websocket_max_queue: 32
    websocket_max_size_bytes: 1048576
```

Memory rule of thumb:

`number_of_connections * websocket_max_queue * average_message_size`

Practical guidance:
- If pipeline memory keeps climbing while queue depth and consumer lag stay low, reduce `websocket_max_queue`.
- If you see rejected or oversized WebSocket frames, review `websocket_max_size_bytes`.
- Tune these together with host memory, enabled symbols, and exchange burst rate.

## 2) Exchange Connectivity Tests

### 2.1 Generic host-level check

```bash
getent hosts <exchange-host>
nc -vz <exchange-host> <port>
openssl s_client -connect <exchange-host>:<port> -servername <exchange-host> </dev/null | head
```

Expected:
- DNS resolves
- TCP connect succeeds
- TLS verify succeeds (`verify return:1`)

### 2.2 OKX symbol validation (REST, terminal-only)

```bash
# 1) Check symbol exists on OKX REST (terminal only)
curl -s "https://www.okx.com/api/v5/public/instruments?instType=SPOT&instId=BTC-USDT"
curl -s "https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=BTC-USDT-SWAP"
```

Expected:
- `"code":"0"`
- `data` array is not empty
- instrument `state` is `"live"`

Notes:
- This validates symbol metadata only (not WebSocket subscription/data flow).
- `instType` and `instId` are case-sensitive and must match OKX format.
- If `data` is empty, symbol/type is invalid or unavailable.

### 2.3 OKX WebSocket endpoint reachability

```bash
getent hosts ws.okx.com
nc -vz ws.okx.com 8443
openssl s_client -connect ws.okx.com:8443 -servername ws.okx.com </dev/null | head
```

## 3) End-to-End Data Flow Checks

### 3.1 Pipeline runtime

```bash
docker compose logs --tail=200 pipeline | grep -Ei "connected|subscribed|disabled|error|exception|okx|bybit|binance"
```

### 3.2 OKX Runtime Subscription Check

```bash
docker compose logs --since=30m pipeline | grep -Ei "okx|Connection disabled|subscription error|connected|subscribed"
```

Expected:
- `connected` and `subscribed` appear for enabled OKX markets.
- `Connection disabled` should not appear for OKX markets you set to `enabled: true`.
- `subscription error` indicates config/symbol/channel mismatch or endpoint issue.

### 3.3 Kafka topic ingestion

```bash
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --list"
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic md.norm.depth.v1 --max-messages 5"
```

### 3.4 ClickHouse sink progress

```bash

docker exec -it clickhouse clickhouse-client -u clickhouse --password clickhouse -d marketdata

docker exec -it clickhouse clickhouse-client -u clickhouse --password clickhouse -d marketdata -q "SELECT count() FROM orderbook_levels_scd"
docker exec -it clickhouse clickhouse-client -u clickhouse --password clickhouse -d marketdata -q "SELECT exchange, market, symbol, count() AS scd_row_count FROM orderbook_levels_scd GROUP BY exchange, market, symbol ORDER BY scd_row_count DESC"
docker exec -it clickhouse clickhouse-client -u clickhouse --password clickhouse -d marketdata -q "
SELECT
    symbol,
    ingest_ts,
    intDiv(valid_from, 1000000) AS source_event_ms,
    round((ingest_ts - intDiv(valid_from, 1000000)) / 1000.0, 3) AS scd_delay_seconds,
    side,
    price,
    qty,
    update_id_to
FROM orderbook_levels_scd
WHERE exchange = 'binance'
  AND market = 'futures'
  AND symbol IN ('BTCUSDT', 'ETHUSDT')
ORDER BY ingest_ts DESC
LIMIT 20"
```

### 3.5 ClickHouse table size (raw vs scd)

```bash
docker exec -it clickhouse clickhouse-client -u clickhouse --password clickhouse -d marketdata -q "
SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS on_disk,
    sum(rows) AS rows
FROM system.parts
WHERE database = 'marketdata'
  AND active
  AND table IN ('orderbook_events_raw','orderbook_levels_scd')
GROUP BY table
ORDER BY table;"
```

```bash
docker exec -it clickhouse sh -lc "du -sh /var/lib/clickhouse 2>/dev/null"

```
## 4) Sequence Gap Monitoring

Use this to verify whether gaps are only startup-related or persistent.

```bash
# 1) See recent gap warnings
docker compose logs --since=10m pipeline | grep "Sequence gap detected"

# 2) Count gaps per stream in last 10 min
docker compose logs --since=10m pipeline | grep "Sequence gap detected" \
| sed -E 's/.*stream=([^ ]+).*/\1/' | sort | uniq -c

# 3) Live gap-rate monitor (every 5s)
watch -n 5 "docker compose logs --since=30s pipeline 2>/dev/null | grep -c 'Sequence gap detected'"

# 4) Confirm sink still progressing
docker compose logs --since=2m processor | tail -n 30
```

Expected:
- Some gaps may appear near startup/bootstrap.
- Gap rate should stabilize after warmup.
- `processor` logs should keep showing writes/progress.

## 5) Kafka Lag and Queue Metrics (CLI)

### 5.1 List groups

```bash
docker exec -it kafka bash -lc "kafka-consumer-groups --bootstrap-server kafka:9092 --list"
```

### 5.2 Describe lag for all groups

```bash
docker exec -it kafka bash -lc "kafka-consumer-groups --bootstrap-server kafka:9092 --all-groups --describe"
```

### 5.3 Watch one group live

```bash
watch -n 5 "docker exec kafka bash -lc 'kafka-consumer-groups --bootstrap-server kafka:9092 --group md.dev.sink.clickhouse.scd.v1 --describe'"
```

### 5.4 Topic partition details

```bash
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --describe --topic md.norm.depth.v1"
```

### 5.5 Approx latest offsets (queue depth input)

```bash
docker exec -it kafka bash -lc "kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic md.norm.depth.v1 --time -1"
```

Interpretation:
- Increasing `LAG` means consumer is falling behind.
- Stable near-zero `LAG` means healthy consumption.


# 1) Find consumer groups
docker exec -it kafka bash -lc "kafka-consumer-groups --bootstrap-server kafka:9092 --list"

# 2) Show per-partition lag for one group
docker exec -it kafka bash -lc "kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group md.dev.sink.clickhouse.scd.v1"

# 3) Watch it live
watch -n 5 "docker exec kafka bash -lc 'kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group md.dev.sink.clickhouse.scd.v1'"
