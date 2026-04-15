# processor.py
import os
import json
from collections import deque
from datetime import datetime, timezone
from kafka import KafkaConsumer
import clickhouse_connect

from contracts.event_ids import make_scd_row_id, resolve_event_id
from consumer_config import resolve_consumer_runtime_config

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_ORDERBOOK = os.getenv(
    "TOPIC_ORDERBOOK",
    os.getenv("KAFKA_TOPIC", "orderbook.btcusdt")
)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", os.getenv("CLICKHOUSE_DB", "marketdata"))
CLICKHOUSE_SCD_TABLE = os.getenv("CLICKHOUSE_SCD_TABLE", os.getenv("CLICKHOUSE_TABLE", "orderbook_levels_scd"))
CLICKHOUSE_RAW_TABLE = os.getenv("CLICKHOUSE_RAW_TABLE", "orderbook_events_raw")
INSERT_BATCH_SIZE = int(os.getenv("INSERT_BATCH_SIZE", "1000"))
INSERT_FLUSH_INTERVAL_MS = int(os.getenv("INSERT_FLUSH_INTERVAL_MS", "2000"))
EVENT_ID_CACHE_SIZE = int(os.getenv("EVENT_ID_CACHE_SIZE", "200000"))
VERSION_KEY_CACHE_SIZE = int(os.getenv("VERSION_KEY_CACHE_SIZE", "500000"))

SYMBOL = os.getenv("SYMBOL", "BTCUSDT")


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def to_int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_non_negative_int(value, default=0) -> int:
    return max(0, to_int(value, default))

def make_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

def ensure_clickhouse_tables(client) -> None:
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_SCD_TABLE}
        (
            exchange       LowCardinality(String),
            market         LowCardinality(String),
            symbol         LowCardinality(String),
            side           LowCardinality(String),
            price          Float64,
            qty            Float64,
            valid_from     UInt64,
            valid_to       Nullable(UInt64),
            opened_by      LowCardinality(String),
            closed_by      Nullable(String),
            update_id_from UInt64,
            update_id_to   UInt64,
            event_id       String,
            ingest_ts      UInt64
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(toDateTime(intDiv(valid_from, 1000000)))
        ORDER BY (exchange, market, symbol, side, price, valid_from)
        """
    )
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_RAW_TABLE}
        (
            event_id                String,
            exchange                LowCardinality(String),
            market                  LowCardinality(String),
            symbol                  LowCardinality(String),
            event_type              LowCardinality(String),
            event_time_ms           UInt64,
            collector_receive_ts_ms Nullable(UInt64),
            snapshot_last_update_id Nullable(UInt64),
            update_id_from          UInt64,
            update_id_to            UInt64,
            bids_json               String,
            asks_json               String,
            ingest_ts               UInt64
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(toDateTime(intDiv(event_time_ms, 1000)))
        ORDER BY (symbol, event_time_ms, update_id_to, event_id)
        """
    )
    client.command(
        f"""
        ALTER TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_RAW_TABLE}
        ADD COLUMN IF NOT EXISTS collector_receive_ts_ms Nullable(UInt64)
        AFTER event_time_ms
        """
    )

def parse_levels(levels):
    # levels like [["70336.90","0.329"], ...]
    out = []
    for p, q in levels:
        out.append((float(p), float(q)))
    return out

def make_valid_from(event_time_ms: int, version_seq: int) -> int:
    """
    Composite BIGINT to guarantee uniqueness even if multiple updates share the same millisecond.
    Keep ms time + inject ordering from version_seq (update_id_to).
    """
    return event_time_ms * 1_000_000 + (version_seq % 1_000_000)

def make_level_version_key(exchange, market, symbol, side, price, valid_from, qty):
    # Logical uniqueness key for one SCD version.
    return (exchange, market, symbol, side, float(price), int(valid_from), float(qty))

def main():
    ch_client = make_clickhouse_client()
    ensure_clickhouse_tables(ch_client)
    consumer_runtime_config = resolve_consumer_runtime_config()

    consumer = KafkaConsumer(
        TOPIC_ORDERBOOK,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset=consumer_runtime_config.auto_offset_reset,
        group_id=consumer_runtime_config.group_id,
        enable_auto_commit=consumer_runtime_config.enable_auto_commit,
    )

    total_rows_processed = 0
    duplicate_open_skipped = 0
    duplicate_close_skipped = 0

    seen_event_ids = set()
    seen_event_ids_q = deque()
    emitted_open_versions = set()
    emitted_open_versions_q = deque()
    emitted_closed_versions = set()
    emitted_closed_versions_q = deque()

    stream_states = {}

    pending_scd_rows = []
    pending_raw_rows = []
    last_flush_ms = now_ms()
    consumed_events = 0

    def flush_rows(force: bool = False) -> None:
        nonlocal pending_scd_rows, pending_raw_rows, last_flush_ms
        due = (now_ms() - last_flush_ms) >= INSERT_FLUSH_INTERVAL_MS
        should_flush = force or due
        wrote_rows = False

        if pending_scd_rows and (force or due or len(pending_scd_rows) >= INSERT_BATCH_SIZE):
            ch_client.insert(
                table=f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_SCD_TABLE}",
                data=pending_scd_rows,
                column_names=[
                    "exchange", "market", "symbol", "side", "price", "qty",
                    "valid_from", "valid_to", "opened_by", "closed_by",
                    "update_id_from", "update_id_to", "event_id", "ingest_ts",
                ],
            )
            pending_scd_rows = []
            should_flush = True
            wrote_rows = True
        if pending_raw_rows and (force or due or len(pending_raw_rows) >= INSERT_BATCH_SIZE):
            ch_client.insert(
                table=f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_RAW_TABLE}",
                data=pending_raw_rows,
                column_names=[
                    "event_id", "exchange", "market", "symbol", "event_type",
                    "event_time_ms", "collector_receive_ts_ms", "snapshot_last_update_id", "update_id_from", "update_id_to",
                    "bids_json", "asks_json", "ingest_ts",
                ],
            )
            pending_raw_rows = []
            should_flush = True
            wrote_rows = True

        if should_flush:
            last_flush_ms = now_ms()
        return wrote_rows

    def flush_and_commit(force: bool = False) -> None:
        wrote_rows = flush_rows(force=force)
        if wrote_rows and not consumer_runtime_config.enable_auto_commit:
            consumer.commit()

    def get_stream_state(stream_key):
        stream_state = stream_states.get(stream_key)
        if stream_state is None:
            stream_state = {
                "book_state": {},
                "snapshot_last_update_id": None,
                "last_applied_u": None,
                "expected_next_u": None,
                "aligned": False,
                "requires_snapshot": False,
                "awaiting_snapshot": False,
            }
            stream_states[stream_key] = stream_state
        return stream_state

    def append_rows(rows) -> None:
        nonlocal pending_scd_rows
        if not rows:
            return
        pending_scd_rows.extend(rows)
        flush_rows(force=False)

    def mark_seen_event(event_id: str) -> bool:
        """Returns True if event_id is duplicate in this process lifetime cache."""
        if not event_id:
            return False
        if event_id in seen_event_ids:
            return True
        seen_event_ids.add(event_id)
        seen_event_ids_q.append(event_id)
        if len(seen_event_ids_q) > EVENT_ID_CACHE_SIZE:
            oldest = seen_event_ids_q.popleft()
            seen_event_ids.discard(oldest)
        return False

    def remember_open_version(version_key) -> bool:
        """Returns True if version was already emitted as open."""
        if version_key in emitted_open_versions:
            return True
        emitted_open_versions.add(version_key)
        emitted_open_versions_q.append(version_key)
        if len(emitted_open_versions_q) > VERSION_KEY_CACHE_SIZE:
            oldest = emitted_open_versions_q.popleft()
            emitted_open_versions.discard(oldest)
        return False

    def remember_closed_version(version_key) -> bool:
        """Returns True if version was already emitted as closed."""
        if version_key in emitted_closed_versions:
            return True
        emitted_closed_versions.add(version_key)
        emitted_closed_versions_q.append(version_key)
        if len(emitted_closed_versions_q) > VERSION_KEY_CACHE_SIZE:
            oldest = emitted_closed_versions_q.popleft()
            emitted_closed_versions.discard(oldest)
        return False

    def append_raw_event(
        event,
        exchange,
        market,
        symbol,
        event_type,
        event_time_ms,
        collector_receive_ts_ms,
        u_from,
        u_to,
        event_id,
        snapshot_last_update_id,
    ):
        nonlocal pending_raw_rows
        pending_raw_rows.append([
            str(event_id or ""),
            exchange,
            market,
            symbol,
            str(event_type or ""),
            int(event_time_ms),
            int(collector_receive_ts_ms) if collector_receive_ts_ms is not None else None,
            int(snapshot_last_update_id) if snapshot_last_update_id is not None else None,
            int(u_from),
            int(u_to),
            json.dumps(event.get("bids", []), separators=(",", ":")),
            json.dumps(event.get("asks", []), separators=(",", ":")),
            now_ms(),
        ])
        flush_rows(force=False)

    print(
        f"[processor] consuming topic={TOPIC_ORDERBOOK} broker={KAFKA_BROKER} "
        f"group_id={consumer_runtime_config.group_id} "
        f"auto_offset_reset={consumer_runtime_config.auto_offset_reset} "
        f"enable_auto_commit={consumer_runtime_config.enable_auto_commit}"
    )
    print(
        f"[processor] writing raw={CLICKHOUSE_DATABASE}.{CLICKHOUSE_RAW_TABLE} "
        f"and scd={CLICKHOUSE_DATABASE}.{CLICKHOUSE_SCD_TABLE} at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
    )

    def close_level(
        exchange,
        market,
        symbol,
        side,
        price,
        prev,
        valid_to,
        u_from,
        u_to,
        source_event_type,
        source_event_id,
    ):
        ingest_ts = now_ms()
        row_event_id = make_scd_row_id(
            exchange=exchange,
            market=market,
            symbol=symbol,
            side=side,
            price=price,
            qty=prev["qty"],
            valid_from_ms=prev["valid_from"],
            valid_to_ms=valid_to,
            source_event_id=source_event_id,
            change_type="close",
            source_sequence=u_to,
            open_sequence=prev.get("open_sequence"),
            close_sequence=u_to,
        )
        return [
            exchange, market, symbol, side, price, prev["qty"],
            prev["valid_from"], valid_to,
            prev["origin_type"], source_event_type,
            u_from, u_to, row_event_id, ingest_ts
        ]

    def open_level(
        exchange,
        market,
        symbol,
        side,
        price,
        qty,
        valid_from,
        source_event_type,
        u_from,
        u_to,
        source_event_id,
        change_type,
    ):
        ingest_ts = now_ms()
        row_event_id = make_scd_row_id(
            exchange=exchange,
            market=market,
            symbol=symbol,
            side=side,
            price=price,
            qty=qty,
            valid_from_ms=valid_from,
            valid_to_ms=None,
            source_event_id=source_event_id,
            change_type=change_type,
            source_sequence=u_to,
            open_sequence=u_to,
            close_sequence=None,
        )
        return [
            exchange, market, symbol, side, price, qty,
            valid_from, None,
            source_event_type, None,
            u_from, u_to, row_event_id, ingest_ts
        ]

    def apply_updates(
        exchange,
        market,
        symbol,
        source_event_type,
        event_time_ms,
        u_from,
        u_to,
        source_event_id,
        bids,
        asks,
        book_state,
    ):
        rows_to_write = []

        valid_from = make_valid_from(int(event_time_ms), int(u_to))

        def apply_side(side: str, updates):
            nonlocal rows_to_write, duplicate_open_skipped, duplicate_close_skipped
            for price, qty in updates:
                key = (side, price)
                prev = book_state.get(key)

                if qty == 0.0:
                    # Close existing level if it exists
                    if prev is not None:
                        prev_key = make_level_version_key(
                            exchange, market, symbol, side, price, prev["valid_from"], prev["qty"]
                        )
                        if not remember_closed_version(prev_key):
                            rows_to_write.append(
                                close_level(
                                    exchange, market, symbol, side, price, prev,
                                    valid_to=valid_from,
                                    u_from=u_from, u_to=u_to,
                                    source_event_type=source_event_type,
                                    source_event_id=source_event_id,
                                )
                            )
                        else:
                            duplicate_close_skipped += 1
                        del book_state[key]
                    continue

                if prev is None:
                    # Open new level and persist as an open SCD row (valid_to empty).
                    open_key = make_level_version_key(
                        exchange, market, symbol, side, price, valid_from, qty
                    )
                    if not remember_open_version(open_key):
                        rows_to_write.append(
                            open_level(
                                exchange, market, symbol, side, price, qty,
                                valid_from,
                                source_event_type,
                                u_from, u_to,
                                source_event_id=source_event_id,
                                change_type="open",
                            )
                        )
                    else:
                        duplicate_open_skipped += 1
                    book_state[key] = {
                        "qty": qty,
                        "valid_from": valid_from,
                        "origin_type": source_event_type,
                    }
                    continue

                # If quantity did not change, ignore (idempotent at level granularity)
                if prev["qty"] == qty:
                    continue

                # Close previous version and open new version
                prev_key = make_level_version_key(
                    exchange, market, symbol, side, price, prev["valid_from"], prev["qty"]
                )
                if not remember_closed_version(prev_key):
                    rows_to_write.append(
                        close_level(
                            exchange, market, symbol, side, price, prev,
                            valid_to=valid_from,
                            u_from=u_from, u_to=u_to,
                            source_event_type=source_event_type,
                            source_event_id=source_event_id,
                        )
                    )
                else:
                    duplicate_close_skipped += 1

                open_key = make_level_version_key(
                    exchange, market, symbol, side, price, valid_from, qty
                )
                if not remember_open_version(open_key):
                    rows_to_write.append(
                        open_level(
                            exchange, market, symbol, side, price, qty,
                            valid_from,
                            source_event_type,
                            u_from, u_to,
                            source_event_id=source_event_id,
                            change_type="update",
                        )
                    )
                else:
                    duplicate_open_skipped += 1
                book_state[key] = {
                    "qty": qty,
                    "valid_from": valid_from,
                    "origin_type": source_event_type,
                }

        apply_side("bid", bids)
        apply_side("ask", asks)
        return rows_to_write, valid_from

    def close_open_levels_for_snapshot(
        exchange,
        market,
        symbol,
        event_time_ms,
        snapshot_u,
        source_event_id,
        book_state,
    ):
        nonlocal duplicate_close_skipped
        rows_to_write = []
        valid_to = make_valid_from(int(event_time_ms), int(snapshot_u))

        for (side, price), prev in list(book_state.items()):
            prev_key = make_level_version_key(
                exchange, market, symbol, side, price, prev["valid_from"], prev["qty"]
            )
            if remember_closed_version(prev_key):
                duplicate_close_skipped += 1
                continue

            rows_to_write.append(
                close_level(
                    exchange,
                    market,
                    symbol,
                    side,
                    price,
                    prev,
                    valid_to=valid_to,
                    u_from=snapshot_u,
                    u_to=snapshot_u,
                    source_event_type="snapshot",
                    source_event_id=source_event_id,
                )
            )

        return rows_to_write

    def reset_to_unaligned(
        stream_state,
        exchange,
        market,
        symbol,
        event_time_ms,
        u_to,
        source_event_id,
        require_snapshot: bool = False,
    ):
        nonlocal total_rows_processed
        reset_close_rows = close_open_levels_for_snapshot(
            exchange=exchange,
            market=market,
            symbol=symbol,
            event_time_ms=event_time_ms,
            snapshot_u=u_to,
            source_event_id=source_event_id,
            book_state=stream_state["book_state"],
        )
        append_rows(reset_close_rows)
        total_rows_processed += len(reset_close_rows)

        if require_snapshot:
            stream_state["awaiting_snapshot"] = stream_state["requires_snapshot"]
        else:
            stream_state["snapshot_last_update_id"] = None
            stream_state["awaiting_snapshot"] = False
        stream_state["last_applied_u"] = None
        stream_state["expected_next_u"] = None
        stream_state["aligned"] = False
        stream_state["book_state"].clear()

    for msg in consumer:
        event = msg.value
        consumed_events += 1

        exchange = event.get("exchange", "binance")
        market = event.get("market", "spot")
        symbol = event.get("symbol", SYMBOL)
        stream_key = f"{exchange}:{market}:{symbol}"
        stream_state = get_stream_state(stream_key)
        book_state = stream_state["book_state"]

        event_type = str(event.get("event_type") or event.get("type") or "delta").lower()
        event_time_ms = to_non_negative_int(event.get("event_time_ms"), now_ms())
        collector_receive_ts_ms = event.get("collector_receive_ts_ms")
        if collector_receive_ts_ms is not None:
            collector_receive_ts_ms = to_non_negative_int(collector_receive_ts_ms, event_time_ms)
        prev_sequence = to_int(event.get("prev_sequence"), None)

        sequence = event.get("sequence")
        u_from_value = event.get("update_id_from", sequence)
        if u_from_value is None:
            u_from_value = sequence
        u_from = to_non_negative_int(u_from_value, 0)

        u_to_value = event.get("update_id_to", sequence)
        if u_to_value is None:
            u_to_value = u_from_value
        u_to = to_non_negative_int(u_to_value, u_from)
        if u_to < u_from:
            u_to = u_from

        snapshot_u_value = event.get("snapshot_last_update_id")
        if snapshot_u_value is None and event_type == "snapshot":
            snapshot_u_value = u_to
        snapshot_u = (
            to_non_negative_int(snapshot_u_value, u_to) if snapshot_u_value is not None else None
        )

        event_id = resolve_event_id(event)
        if not event_id:
            event_id = f"{exchange}|{market}|{symbol}|{event_type}|{u_from}|{u_to}|{event_time_ms}"

        append_raw_event(
            event=event,
            exchange=exchange,
            market=market,
            symbol=symbol,
            event_type=event_type,
            event_time_ms=event_time_ms,
            collector_receive_ts_ms=collector_receive_ts_ms,
            u_from=u_from,
            u_to=u_to,
            event_id=event_id,
            snapshot_last_update_id=snapshot_u,
        )
        if mark_seen_event(str(event_id) if event_id is not None else ""):
            flush_and_commit(force=False)
            continue
        flush_rows(force=False)

        bids = parse_levels(event.get("bids", []))
        asks = parse_levels(event.get("asks", []))

        # --- SNAPSHOT ---
        if event_type == "snapshot":
            incoming_snapshot_u = snapshot_u if snapshot_u is not None else u_to

            # Ignore stale/replayed snapshots once already aligned on newer updates.
            if (
                stream_state["aligned"]
                and stream_state["last_applied_u"] is not None
                and incoming_snapshot_u <= stream_state["last_applied_u"]
            ):
                print(
                    f"[processor] stream={stream_key} ignored stale snapshot "
                    f"lastUpdateId={incoming_snapshot_u} last_applied_u={stream_state['last_applied_u']}"
                )
                flush_and_commit(force=False)
                continue

            stream_state["snapshot_last_update_id"] = incoming_snapshot_u
            stream_state["requires_snapshot"] = True
            stream_state["awaiting_snapshot"] = False

            # After a snapshot we are NOT aligned yet.
            stream_state["aligned"] = False
            stream_state["last_applied_u"] = incoming_snapshot_u
            stream_state["expected_next_u"] = None  # set after we accept first bridging delta

            # Close current open levels before rebuilding state from the snapshot.
            snapshot_close_rows = close_open_levels_for_snapshot(
                exchange=exchange,
                market=market,
                symbol=symbol,
                event_time_ms=event_time_ms,
                snapshot_u=incoming_snapshot_u,
                source_event_id=event_id,
                book_state=book_state,
            )
            append_rows(snapshot_close_rows)
            total_rows_processed += len(snapshot_close_rows)

            # Hard reset state to snapshot
            book_state.clear()

            # Apply snapshot as the new current state after closing the old one.
            rows, _ = apply_updates(
                exchange=exchange,
                market=market,
                symbol=symbol,
                source_event_type="snapshot",
                event_time_ms=event_time_ms,
                u_from=incoming_snapshot_u,
                u_to=incoming_snapshot_u,
                source_event_id=event_id,
                bids=bids,
                asks=asks,
                book_state=book_state,
            )
            append_rows(rows)
            total_rows_processed += len(rows)

            print(
                f"[processor] stream={stream_key} snapshot loaded "
                f"lastUpdateId={incoming_snapshot_u} open_levels={len(book_state)}"
            )
            flush_and_commit(force=False)
            continue

        # --- DELTA ---
        if event_type != "delta":
            flush_and_commit(force=False)
            continue

        if stream_state["awaiting_snapshot"]:
            flush_and_commit(force=False)
            continue

        # Bootstrapping mode for streams that do not provide snapshots in this topic.
        if stream_state["snapshot_last_update_id"] is None:
            rows, _ = apply_updates(
                exchange=exchange,
                market=market,
                symbol=symbol,
                source_event_type="delta",
                event_time_ms=event_time_ms,
                u_from=u_from,
                u_to=u_to,
                source_event_id=event_id,
                bids=bids,
                asks=asks,
                book_state=book_state,
            )
            append_rows(rows)
            total_rows_processed += len(rows)
            stream_state["snapshot_last_update_id"] = u_to
            stream_state["aligned"] = True
            stream_state["last_applied_u"] = u_to
            stream_state["expected_next_u"] = u_to + 1
            stream_state["awaiting_snapshot"] = False
            print(
                f"[processor] stream={stream_key} bootstrapped from delta u={u_to} "
                f"rows={len(rows)} open_levels={len(book_state)}"
            )
            flush_and_commit(force=False)
            continue

        # Ignore fully stale deltas (idempotency / replay safety)
        if stream_state["last_applied_u"] is not None and u_to <= stream_state["last_applied_u"]:
            flush_and_commit(force=False)
            continue

        target = stream_state["snapshot_last_update_id"] + 1

        # 1) Before alignment: accept ONLY a bridging delta (U <= target <= u).
        if not stream_state["aligned"]:
            if u_to < target:
                # Entirely before the snapshot -> ignore
                continue

            if u_from <= target <= u_to:
                rows, _ = apply_updates(
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    source_event_type="delta",
                    event_time_ms=event_time_ms,
                    u_from=u_from,
                    u_to=u_to,
                    source_event_id=event_id,
                    bids=bids,
                    asks=asks,
                    book_state=book_state,
                )
                append_rows(rows)

                stream_state["aligned"] = True
                stream_state["last_applied_u"] = u_to
                stream_state["expected_next_u"] = u_to + 1

                print(
                    f"[processor] stream={stream_key} aligned on delta U={u_from}..u={u_to} target={target} "
                    f"closed_rows={len(rows)} open_levels={len(book_state)}"
                )
                flush_and_commit(force=False)
                continue

            # Delta is ahead of target but doesn't bridge -> resync needed
            print(
                f"[processor] stream={stream_key} No bridging delta: target={target} "
                f"got U={u_from}..u={u_to}. Resync required."
            )
            reset_to_unaligned(
                stream_state,
                exchange=exchange,
                market=market,
                symbol=symbol,
                event_time_ms=event_time_ms,
                u_to=u_to,
                source_event_id=event_id,
                require_snapshot=True,
            )
            flush_and_commit(force=False)
            continue

        # 2) After alignment: handle overlap, stale, and gaps.
        if stream_state["expected_next_u"] is not None:
            if u_to < stream_state["expected_next_u"]:
                # Fully stale / replayed delta
                continue

            if prev_sequence is not None:
                if prev_sequence != stream_state["last_applied_u"]:
                    print(
                        f"[processor] stream={stream_key} GAP: expected prev={stream_state['last_applied_u']} "
                        f"but got prev={prev_sequence} U={u_from}..u={u_to}. Resync required."
                    )
                    reset_to_unaligned(
                        stream_state,
                        exchange=exchange,
                        market=market,
                        symbol=symbol,
                        event_time_ms=event_time_ms,
                        u_to=u_to,
                        source_event_id=event_id,
                        require_snapshot=True,
                    )
                    flush_and_commit(force=False)
                    continue
            elif u_from > stream_state["expected_next_u"]:
                # Real gap: we are missing updates
                print(
                    f"[processor] stream={stream_key} GAP: expected U={stream_state['expected_next_u']} "
                    f"but got U={u_from}..u={u_to}. Resync required."
                )
                reset_to_unaligned(
                    stream_state,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    event_time_ms=event_time_ms,
                    u_to=u_to,
                    source_event_id=event_id,
                    require_snapshot=True,
                )
                flush_and_commit(force=False)
                continue
            # Overlap is OK: u_from <= expected_next_u <= u_to

        rows, _ = apply_updates(
            exchange=exchange,
            market=market,
            symbol=symbol,
            source_event_type="delta",
            event_time_ms=event_time_ms,
            u_from=u_from,
            u_to=u_to,
            source_event_id=event_id,
            bids=bids,
            asks=asks,
            book_state=book_state,
        )
        append_rows(rows)
        total_rows_processed += len(rows)

        stream_state["last_applied_u"] = u_to
        stream_state["expected_next_u"] = u_to + 1

        if rows:
            print(
                f"[processor] stream={stream_key} wrote closed_rows={len(rows)} "
                f"total_rows_processed={total_rows_processed} at u_to={u_to} open_levels={len(book_state)}"
            )
        if consumed_events % 200 == 0:
            print(
                f"[processor] progress consumed_events={consumed_events} "
                f"pending_raw={len(pending_raw_rows)} pending_scd={len(pending_scd_rows)} "
                f"duplicate_open_skipped={duplicate_open_skipped} duplicate_close_skipped={duplicate_close_skipped}"
            )
        flush_and_commit(force=False)

if __name__ == "__main__":
    main()
