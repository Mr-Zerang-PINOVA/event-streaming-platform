# ClickHouse Validation Queries

This directory keeps reusable ClickHouse checks out of the main project docs.

## Files

- `validation_queries.sql`: copy/paste-ready queries for row counts, symbol coverage, snapshot checks, duplicate checks, ingest-delay checks, JSON extraction, and single-level inspection.

## Tables Covered

- `marketdata.orderbook_events_raw`
- `marketdata.orderbook_levels_scd`

## How To Use

Run a single query:

```bash
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM marketdata.orderbook_levels_scd"
```

Open the SQL file in CloudBeaver / DBeaver and run one section at a time:

- Basic counts
- Coverage checks
- Snapshot persistence
- Ingest delay spot checks
- Duplicate and lifecycle validation
- One-level deep inspection

## Why This Lives Here

Keeping these queries in a dedicated SQL file makes them easier to reuse during troubleshooting and avoids turning the main runbook into a long wall of SQL.

## Notes

- `event_type` is usually `snapshot` or `delta`.
- `valid_from` is a composite version identifier produced by the processor, not a human timestamp.
- `bids_json` and `asks_json` store arrays like `[["price","qty"], ...]`.
