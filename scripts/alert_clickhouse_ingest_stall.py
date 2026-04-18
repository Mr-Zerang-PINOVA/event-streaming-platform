#!/usr/bin/env python3
"""Send Telegram alerts when ClickHouse ingest stalls for too long."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib import error, request


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Config:
    clickhouse_scheme: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_table: str
    clickhouse_user: str
    clickhouse_password: str
    telegram_bot_token: str
    telegram_chat_id: str
    alert_name: str
    max_idle_seconds: int
    poll_interval_seconds: int
    timeout_seconds: int


@dataclass(frozen=True)
class TableStatus:
    row_count: int
    last_ingest_ts_ms: Optional[int]
    clickhouse_now_ms: int


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor ClickHouse table freshness and send a Telegram alert "
            "when no new rows arrive for too long."
        )
    )
    parser.add_argument(
        "--clickhouse-scheme",
        default=os.getenv("CLICKHOUSE_SCHEME", "http"),
        help="ClickHouse HTTP scheme. Default: %(default)s",
    )
    parser.add_argument(
        "--clickhouse-host",
        default=os.getenv("CLICKHOUSE_HOST", "localhost"),
        help="ClickHouse host. Default: %(default)s",
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        help="ClickHouse HTTP port. Default: %(default)s",
    )
    parser.add_argument(
        "--clickhouse-database",
        default=os.getenv("CLICKHOUSE_DATABASE", "marketdata"),
        help="ClickHouse database name. Default: %(default)s",
    )
    parser.add_argument(
        "--clickhouse-table",
        default=os.getenv("CLICKHOUSE_TABLE", "orderbook_levels_scd"),
        help="ClickHouse table name. Default: %(default)s",
    )
    parser.add_argument(
        "--clickhouse-user",
        default=os.getenv("CLICKHOUSE_USER", "clickhouse"),
        help="ClickHouse user. Default: %(default)s",
    )
    parser.add_argument(
        "--clickhouse-password",
        default=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        help="ClickHouse password. Default: %(default)s",
    )
    parser.add_argument(
        "--telegram-bot-token",
        default=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        help="Telegram bot token. Can also be set via TELEGRAM_BOT_TOKEN.",
    )
    parser.add_argument(
        "--telegram-chat-id",
        default=os.getenv("TELEGRAM_CHAT_ID", ""),
        help="Telegram chat id. Can also be set via TELEGRAM_CHAT_ID.",
    )
    parser.add_argument(
        "--alert-name",
        default=os.getenv("ALERT_NAME", "marketdata-scd-ingest"),
        help="Short label added to Telegram messages. Default: %(default)s",
    )
    parser.add_argument(
        "--max-idle-seconds",
        type=int,
        default=int(os.getenv("MAX_IDLE_SECONDS", "300")),
        help="Alert threshold in seconds. Default: %(default)s",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
        help="Polling interval in seconds. Default: %(default)s",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.getenv("HTTP_TIMEOUT_SECONDS", "10")),
        help="HTTP timeout for ClickHouse and Telegram requests. Default: %(default)s",
    )
    args = parser.parse_args()

    for name, value in (
        ("clickhouse_database", args.clickhouse_database),
        ("clickhouse_table", args.clickhouse_table),
    ):
        if not IDENTIFIER_RE.match(value):
            parser.error(
                f"{name} must match {IDENTIFIER_RE.pattern!r}; received {value!r}"
            )

    if not args.telegram_bot_token:
        parser.error(
            "telegram bot token is required; set --telegram-bot-token or TELEGRAM_BOT_TOKEN"
        )
    if not args.telegram_chat_id:
        parser.error(
            "telegram chat id is required; set --telegram-chat-id or TELEGRAM_CHAT_ID"
        )
    if args.max_idle_seconds <= 0:
        parser.error("--max-idle-seconds must be greater than 0")
    if args.poll_interval_seconds <= 0:
        parser.error("--poll-interval-seconds must be greater than 0")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be greater than 0")

    return Config(
        clickhouse_scheme=args.clickhouse_scheme,
        clickhouse_host=args.clickhouse_host,
        clickhouse_port=args.clickhouse_port,
        clickhouse_database=args.clickhouse_database,
        clickhouse_table=args.clickhouse_table,
        clickhouse_user=args.clickhouse_user,
        clickhouse_password=args.clickhouse_password,
        telegram_bot_token=args.telegram_bot_token,
        telegram_chat_id=args.telegram_chat_id,
        alert_name=args.alert_name,
        max_idle_seconds=args.max_idle_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        timeout_seconds=args.timeout_seconds,
    )


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def format_utc_ms(value_ms: Optional[int]) -> str:
    if value_ms is None:
        return "n/a"
    return datetime.fromtimestamp(value_ms / 1000.0, tz=timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60.0:.1f}m"
    return f"{seconds / 3600.0:.2f}h"


def build_clickhouse_query(config: Config) -> str:
    return f"""
SELECT
    count() AS row_count,
    nullIf(max(ingest_ts), 0) AS last_ingest_ts,
    toUnixTimestamp64Milli(now64(3)) AS clickhouse_now_ms
FROM {config.clickhouse_database}.{config.clickhouse_table}
FORMAT JSON
""".strip()


def build_basic_auth_header(username: str, password: str) -> Optional[str]:
    if not username and not password:
        return None
    credentials = f"{username}:{password}".encode("utf-8")
    return base64.b64encode(credentials).decode("ascii")


def fetch_table_status(config: Config) -> TableStatus:
    query = build_clickhouse_query(config).encode("utf-8")
    url = f"{config.clickhouse_scheme}://{config.clickhouse_host}:{config.clickhouse_port}/"
    http_request = request.Request(url=url, data=query, method="POST")
    http_request.add_header("Content-Type", "text/plain; charset=utf-8")

    auth_header = build_basic_auth_header(
        config.clickhouse_user, config.clickhouse_password
    )
    if auth_header:
        http_request.add_header("Authorization", f"Basic {auth_header}")

    with request.urlopen(http_request, timeout=config.timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    try:
        row = payload["data"][0]
        row_count = int(row["row_count"])
        last_ingest_raw = row.get("last_ingest_ts")
        clickhouse_now_ms = int(row["clickhouse_now_ms"])
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise RuntimeError(f"unexpected ClickHouse response: {payload!r}") from exc

    last_ingest_ts_ms = int(last_ingest_raw) if last_ingest_raw is not None else None
    return TableStatus(
        row_count=row_count,
        last_ingest_ts_ms=last_ingest_ts_ms,
        clickhouse_now_ms=clickhouse_now_ms,
    )


def send_telegram_message(config: Config, text: str) -> None:
    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
    body = json.dumps(
        {
            "chat_id": config.telegram_chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")
    http_request = request.Request(url=url, data=body, method="POST")
    http_request.add_header("Content-Type", "application/json; charset=utf-8")
    with request.urlopen(http_request, timeout=config.timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if not payload.get("ok"):
        raise RuntimeError(f"telegram sendMessage failed: {payload!r}")


def build_alert_message(config: Config, status: TableStatus, idle_seconds: float) -> str:
    return "\n".join(
        [
            f"{config.alert_name}: ClickHouse ingest stalled",
            f"table={config.clickhouse_database}.{config.clickhouse_table}",
            f"idle_for={format_duration(idle_seconds)}",
            f"threshold={format_duration(config.max_idle_seconds)}",
            f"row_count={status.row_count}",
            f"last_ingest_utc={format_utc_ms(status.last_ingest_ts_ms)}",
            f"checked_utc={format_utc_ms(status.clickhouse_now_ms)}",
            f"clickhouse={config.clickhouse_scheme}://{config.clickhouse_host}:{config.clickhouse_port}",
        ]
    )


def build_recovery_message(
    config: Config,
    status: TableStatus,
    idle_seconds: float,
    alert_active_seconds: Optional[float],
) -> str:
    lines = [
        f"{config.alert_name}: ClickHouse ingest recovered",
        f"table={config.clickhouse_database}.{config.clickhouse_table}",
        f"row_count={status.row_count}",
        f"latest_ingest_age={format_duration(idle_seconds)}",
        f"last_ingest_utc={format_utc_ms(status.last_ingest_ts_ms)}",
        f"checked_utc={format_utc_ms(status.clickhouse_now_ms)}",
    ]
    if alert_active_seconds is not None:
        lines.append(f"alert_active_for={format_duration(alert_active_seconds)}")
    return "\n".join(lines)


def log(message: str) -> None:
    print(f"[{now_utc_iso()}] {message}", flush=True)


def log_error(message: str) -> None:
    print(f"[{now_utc_iso()}] {message}", file=sys.stderr, flush=True)


def monitor(config: Config) -> int:
    last_seen_row_count: Optional[int] = None
    last_seen_ingest_ts_ms: Optional[int] = None
    alert_active = False
    alert_started_monotonic: Optional[float] = None

    while True:
        try:
            status = fetch_table_status(config)
        except (RuntimeError, ValueError, error.HTTPError, error.URLError, TimeoutError) as exc:
            log_error(f"monitor check failed: {exc}")
            time.sleep(config.poll_interval_seconds)
            continue

        if status.last_ingest_ts_ms is None:
            log(
                "table has no rows yet; "
                f"table={config.clickhouse_database}.{config.clickhouse_table}"
            )
            time.sleep(config.poll_interval_seconds)
            continue

        idle_seconds = max(
            (status.clickhouse_now_ms - status.last_ingest_ts_ms) / 1000.0,
            0.0,
        )
        has_new_rows = (
            last_seen_row_count is None
            or last_seen_ingest_ts_ms is None
            or status.row_count > last_seen_row_count
            or status.last_ingest_ts_ms > last_seen_ingest_ts_ms
        )

        if has_new_rows:
            if alert_active:
                alert_active_seconds = None
                if alert_started_monotonic is not None:
                    alert_active_seconds = time.monotonic() - alert_started_monotonic
                try:
                    send_telegram_message(
                        config,
                        build_recovery_message(
                            config,
                            status,
                            idle_seconds,
                            alert_active_seconds,
                        ),
                    )
                    log(
                        "sent recovery message; "
                        f"row_count={status.row_count} last_ingest_utc={format_utc_ms(status.last_ingest_ts_ms)}"
                    )
                    alert_active = False
                    alert_started_monotonic = None
                    last_seen_row_count = status.row_count
                    last_seen_ingest_ts_ms = status.last_ingest_ts_ms
                except (RuntimeError, ValueError, error.HTTPError, error.URLError, TimeoutError) as exc:
                    log_error(f"failed to send recovery message: {exc}")
                    time.sleep(config.poll_interval_seconds)
                    continue
            else:
                log(
                    "healthy; "
                    f"row_count={status.row_count} idle_for={format_duration(idle_seconds)} "
                    f"last_ingest_utc={format_utc_ms(status.last_ingest_ts_ms)}"
                )
                last_seen_row_count = status.row_count
                last_seen_ingest_ts_ms = status.last_ingest_ts_ms

            time.sleep(config.poll_interval_seconds)
            continue

        if idle_seconds >= config.max_idle_seconds and not alert_active:
            try:
                send_telegram_message(
                    config,
                    build_alert_message(config, status, idle_seconds),
                )
                alert_active = True
                alert_started_monotonic = time.monotonic()
                log(
                    "sent stall alert; "
                    f"row_count={status.row_count} idle_for={format_duration(idle_seconds)}"
                )
            except (RuntimeError, ValueError, error.HTTPError, error.URLError, TimeoutError) as exc:
                log_error(f"failed to send stall alert: {exc}")
        else:
            log(
                "waiting; "
                f"row_count={status.row_count} idle_for={format_duration(idle_seconds)} "
                f"threshold={format_duration(config.max_idle_seconds)} "
                f"alert_active={alert_active}"
            )

        time.sleep(config.poll_interval_seconds)


def main() -> int:
    config = parse_args()
    log(
        "starting ClickHouse ingest stall monitor; "
        f"table={config.clickhouse_database}.{config.clickhouse_table} "
        f"threshold={format_duration(config.max_idle_seconds)} "
        f"poll_interval={format_duration(config.poll_interval_seconds)}"
    )
    return monitor(config)


if __name__ == "__main__":
    raise SystemExit(main())
