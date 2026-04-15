-- ============================================================
-- ClickHouse validation queries for marketdata pipeline
--
-- Tables:
--   marketdata.orderbook_events_raw   -- raw snapshot/delta events
--   marketdata.orderbook_levels_scd   -- SCD2 order book levels
--

-- ============================================================

docker exec -it clickhouse clickhouse-client -u clickhouse --password clickhouse -d marketdata

-- ------------------------------------------------------------
-- 1) Basic row counts
-- ------------------------------------------------------------

SELECT count() AS scd_rows
FROM marketdata.orderbook_levels_scd;

SELECT count() AS raw_rows
FROM marketdata.orderbook_events_raw;


-- ------------------------------------------------------------
-- 2) Coverage: symbols, markets, exchanges
-- ------------------------------------------------------------

SELECT
   exchange,
    symbol,
    market,
    count() AS scd_row_count
FROM marketdata.orderbook_levels_scd
GROUP BY exchange, symbol, market
ORDER BY scd_row_count DESC;


-- ------------------------------------------------------------
-- 3) Lifecycle source checks
-- opened_by is usually 'snapshot' or 'delta'
-- ------------------------------------------------------------

SELECT
    opened_by,
    symbol,
    market,
    count() AS rows_cnt
FROM marketdata.orderbook_levels_scd
GROUP BY opened_by, symbol, market
ORDER BY rows_cnt DESC, opened_by, symbol;

SELECT
    opened_by,
    count() AS rows_cnt
FROM marketdata.orderbook_levels_scd
GROUP BY opened_by
ORDER BY rows_cnt DESC, opened_by;


-- ------------------------------------------------------------
-- 4) Quick inspection samples
-- ------------------------------------------------------------

SELECT *
FROM marketdata.orderbook_levels_scd
LIMIT 10;

SELECT *
FROM marketdata.orderbook_events_raw
LIMIT 10;

SELECT *
FROM marketdata.orderbook_events_raw
WHERE event_type != 'delta'
ORDER BY event_time_ms DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 5) Decode bid levels from raw JSON
-- bids_json contains arrays like [["price","qty"], ...]
-- ------------------------------------------------------------

SELECT
    symbol,
    event_type,
    event_time_ms,
    level_arr[1] AS price,
    level_arr[2] AS qty
FROM
(
    SELECT
        symbol,
        event_type,
        event_time_ms,
        JSONExtract(level_raw, 'Array(String)') AS level_arr
    FROM
    (
        SELECT
            symbol,
            event_type,
            event_time_ms,
            arrayJoin(JSONExtractArrayRaw(bids_json)) AS level_raw
        FROM marketdata.orderbook_events_raw
    )
)
LIMIT 10;


-- ------------------------------------------------------------
-- 6) Duplicate detection in SCD table
-- ------------------------------------------------------------

-- Closed versions duplicated by the same business key
SELECT
    exchange,
    market,
    symbol,
    side,
    price,
    valid_from,
    valid_to,
    count() AS dup_rows
FROM marketdata.orderbook_levels_scd
WHERE valid_to IS NOT NULL
GROUP BY exchange, market, symbol, side, price, valid_from, valid_to
HAVING dup_rows > 1
ORDER BY dup_rows DESC, symbol, side, price, valid_from;


SELECT
    exchange,
    market,
    symbol,
    side,
    price,
    valid_from,
    valid_to,
    count() AS dup_rows
FROM marketdata.orderbook_levels_scd
GROUP BY
    exchange,
    market,
    symbol,
    side,
    price,
    valid_from,
    valid_to
HAVING dup_rows > 1
ORDER BY
    dup_rows DESC,
    symbol ASC,
    side ASC,
    price ASC,
    valid_from ASC,
    valid_to ASC;


-- Compare total rows vs unique logical versions
SELECT
    count() AS total_rows,
    uniqExact(exchange, market, symbol, side, price, valid_from, qty) AS unique_versions
FROM marketdata.orderbook_levels_scd;

-- Find versions with multiple open rows or multiple close rows
SELECT
    exchange,
    market,
    symbol,
    side,
    price,
    valid_from,
    qty,
    countIf(valid_to IS NULL) AS open_cnt,
    countIf(valid_to IS NOT NULL) AS close_cnt
FROM marketdata.orderbook_levels_scd
GROUP BY exchange, market, symbol, side, price, valid_from, qty
HAVING open_cnt > 1 OR close_cnt > 1
ORDER BY greatest(open_cnt, close_cnt) DESC
LIMIT 100;

-- Summary of lifecycle consistency
SELECT
    countIf(open_cnt = 1 AND close_cnt = 1) AS versions_with_both_open_and_close,
    countIf(open_cnt = 1 AND close_cnt = 0) AS currently_open_versions,
    countIf(open_cnt = 0 AND close_cnt = 1) AS close_without_open
FROM
(
    SELECT
        exchange,
        market,
        symbol,
        side,
        price,
        valid_from,
        qty,
        countIf(valid_to IS NULL) AS open_cnt,
        countIf(valid_to IS NOT NULL) AS close_cnt
    FROM marketdata.orderbook_levels_scd
    GROUP BY exchange, market, symbol, side, price, valid_from, qty
);


-- ------------------------------------------------------------
-- 7) Closed/open row inspection
-- ------------------------------------------------------------

SELECT
    exchange,
    market,
    symbol,
    side,
    price,
    valid_from,
    valid_to
FROM marketdata.orderbook_levels_scd
WHERE valid_to IS NOT NULL
LIMIT 10;

SELECT *
FROM marketdata.orderbook_levels_scd
WHERE opened_by = 'snapshot'
LIMIT 10;

SELECT *
FROM marketdata.orderbook_levels_scd
WHERE opened_by = 'delta'
LIMIT 10;


-- ------------------------------------------------------------
-- 8) Snapshot persistence check
-- ------------------------------------------------------------

SELECT
    event_time_ms,
    event_type,
    symbol,
    update_id_from,
    update_id_to,
    snapshot_last_update_id
FROM marketdata.orderbook_events_raw
WHERE event_type = 'snapshot'
ORDER BY event_time_ms DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 9) Near-real-time delay spot check
-- Compare source event time and collector receive time to DB ingest time
-- ------------------------------------------------------------

SELECT
    exchange,
    market,
    symbol,
    event_type,
    event_time_ms,
    collector_receive_ts_ms,
    ingest_ts,
    round((ingest_ts - event_time_ms) / 1000.0, 3) AS source_to_db_seconds,
    round((ingest_ts - collector_receive_ts_ms) / 1000.0, 3) AS collector_to_db_seconds,
    update_id_to
FROM marketdata.orderbook_events_raw
WHERE exchange = 'binance'
  AND market = 'futures'
  AND symbol IN ('BTCUSDT', 'ETHUSDT')
ORDER BY ingest_ts DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 10) Inspect one exact SCD level version
-- Replace values as needed
-- ------------------------------------------------------------

SELECT
    exchange,
    market,
    symbol,
    side,
    price,
    qty,
    valid_from,
    valid_to,
    opened_by,
    closed_by,
    update_id_from,
    update_id_to,
    event_id
FROM marketdata.orderbook_levels_scd
WHERE exchange = 'binance'
  AND market = 'futures'
  AND symbol = 'BTCUSDT'
  AND side = 'ask'
  AND price = 66370.4
  AND valid_from = 1774950716764840270;

SELECT
    exchange,
    market,
    symbol,
    side,
    price,
    valid_from,
    valid_to,
    count() AS rows_cnt
FROM marketdata.orderbook_levels_scd
WHERE exchange = 'binance'
  AND market = 'futures'
  AND symbol = 'BTCUSDT'
  AND side = 'ask'
  AND price = 66370.4
  AND valid_from = 1774950716764840270
GROUP BY exchange, market, symbol, side, price, valid_from, valid_to
ORDER BY rows_cnt DESC;
