import asyncio
import contextlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

import websockets

from .base_collector import BaseCollector


class BinanceCollector(BaseCollector):
    def __init__(
        self,
        depth_stream: str = "depth@100ms",
        snapshot_limit: int = 1000,
        snapshot_timeout_seconds: float = 10.0,
        alignment_timeout_seconds: float = 5.0,
        max_alignment_snapshot_refreshes: int = 3,
        warn_event_age_ms: int = 30000,
        max_event_age_ms: int = 180000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.depth_stream = depth_stream
        self.snapshot_limit = int(snapshot_limit)
        self.snapshot_timeout_seconds = float(snapshot_timeout_seconds)
        self.alignment_timeout_seconds = float(alignment_timeout_seconds)
        self.max_alignment_snapshot_refreshes = max(1, int(max_alignment_snapshot_refreshes))
        self.warn_event_age_ms = max(0, int(warn_event_age_ms))
        self.max_event_age_ms = max(0, int(max_event_age_ms))
        self._last_sequence: Optional[int] = None
        self._awaiting_bridge = False
        self._last_event_age_warning_ms = 0

    async def subscribe(self, websocket_client: Any) -> None:
        stream_name = f"{self.symbol.lower()}@{self.depth_stream}"
        subscribe_message = {"method": "SUBSCRIBE", "params": [stream_name], "id": 1}
        await websocket_client.send(json.dumps(subscribe_message))
        self.logger.info("Subscribed stream=%s", stream_name)

    async def run(self) -> None:
        backoff = self.reconnect_base_seconds
        while not self._stop_event.is_set():
            try:
                self._last_sequence = None
                self._awaiting_bridge = False
                self.logger.info("Connecting ws_url=%s", self.ws_url)
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=self.ping_interval_seconds,
                    ping_timeout=self.ping_timeout_seconds,
                    max_queue=self.websocket_max_queue,
                ) as websocket_client:
                    self.logger.info("Connected. Subscribing symbol=%s", self.symbol)
                    await self.subscribe(websocket_client)
                    backoff = self.reconnect_base_seconds
                    await self._stream_with_snapshot_alignment(websocket_client)
            except asyncio.CancelledError:
                raise
            except _BinanceResyncRequired as exc:
                self.logger.warning("Resync required symbol=%s reason=%s", self.symbol, exc)
            except Exception:
                self.logger.exception("Collector error. Reconnect is scheduled.")

            if self._stop_event.is_set():
                break

            sleep_seconds = min(backoff, self.reconnect_max_seconds)
            jitter = random.uniform(0.0, sleep_seconds * 0.2)
            sleep_seconds += jitter
            self.logger.warning("Reconnecting in %.2f seconds", sleep_seconds)
            await asyncio.sleep(sleep_seconds)
            backoff = min(backoff * 2, self.reconnect_max_seconds)

        self.logger.info("Collector stopped")

    async def _stream_with_snapshot_alignment(self, websocket_client: Any) -> None:
        buffered_payloads: List[Dict[str, Any]] = []
        snapshot_task = asyncio.create_task(self._fetch_snapshot())
        snapshot: Optional[Dict[str, Any]] = None
        aligned = False
        snapshot_ready_at_monotonic: Optional[float] = None
        alignment_snapshot_refreshes = 0

        try:
            async for raw_message in websocket_client:
                if self._stop_event.is_set():
                    break

                payload = _load_json(raw_message)
                if payload is None:
                    continue

                if not self._is_depth_payload(payload):
                    continue

                payload["_collector_receive_ts_ms"] = int(time.time() * 1000)
                self._check_event_freshness(payload)
                buffered_payloads.append(payload)
                if not aligned:
                    if not snapshot_task.done():
                        continue

                    if snapshot is None:
                        snapshot = await snapshot_task
                        snapshot_ready_at_monotonic = time.monotonic()
                        alignment_snapshot_refreshes = 0
                    buffered_payloads = self._prune_buffer(snapshot, buffered_payloads)
                    if self._snapshot_is_too_old(snapshot, buffered_payloads):
                        if alignment_snapshot_refreshes >= self.max_alignment_snapshot_refreshes:
                            raise _BinanceResyncRequired(
                                "snapshot too old for buffered deltas "
                                f"after_refreshes={alignment_snapshot_refreshes}"
                            )
                        snapshot = await self._refresh_alignment_snapshot(snapshot, buffered_payloads)
                        snapshot_ready_at_monotonic = time.monotonic()
                        alignment_snapshot_refreshes += 1
                        buffered_payloads = self._prune_buffer(snapshot, buffered_payloads)
                    aligned_index = self._find_alignment_index(snapshot, buffered_payloads)
                    if aligned_index is None:
                        self._raise_if_alignment_stalled(
                            snapshot=snapshot,
                            buffered_payloads=buffered_payloads,
                            snapshot_ready_at_monotonic=snapshot_ready_at_monotonic,
                        )
                        continue

                    aligned_updates = buffered_payloads[aligned_index:]
                    await self._publish_snapshot(snapshot)
                    for aligned_payload in aligned_updates:
                        await self._publish_live_delta(aligned_payload)

                    buffered_payloads.clear()
                    aligned = True
                    continue

                await self._publish_live_delta(payload)
        finally:
            if not snapshot_task.done():
                snapshot_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await snapshot_task

    def extract_depth_updates(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        update = self._extract_depth_update(payload)
        return [update] if update is not None else []

    async def _fetch_snapshot(self) -> Dict[str, Any]:
        snapshot = await asyncio.to_thread(self._fetch_snapshot_sync)
        self.logger.info(
            "Fetched bootstrap snapshot symbol=%s market=%s lastUpdateId=%s",
            self.symbol,
            self.market,
            snapshot["lastUpdateId"],
        )
        return snapshot

    def _fetch_snapshot_sync(self) -> Dict[str, Any]:
        params = urllib.parse.urlencode({"symbol": self.symbol.upper(), "limit": self.snapshot_limit})
        request_url = f"{self._snapshot_url()}?{params}"
        request = urllib.request.Request(
            request_url,
            headers={"Accept": "application/json", "User-Agent": "market-depth-pipeline"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.snapshot_timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            raise _BinanceResyncRequired(f"snapshot fetch failed: {exc}") from exc

        snapshot_sequence = _safe_int(payload.get("lastUpdateId"))
        if snapshot_sequence is None:
            raise _BinanceResyncRequired("snapshot missing lastUpdateId")

        return {
            "lastUpdateId": snapshot_sequence,
            "bids": payload.get("bids", []),
            "asks": payload.get("asks", []),
        }

    async def _publish_snapshot(self, snapshot: Dict[str, Any]) -> None:
        snapshot_sequence = snapshot["lastUpdateId"]
        snapshot_event = {
            "event_type": "snapshot",
            "event_time_ms": int(time.time() * 1000),
            "collector_receive_ts_ms": int(time.time() * 1000),
            "sequence": snapshot_sequence,
            "prev_sequence": None,
            "snapshot_last_update_id": snapshot_sequence,
            "update_id_from": snapshot_sequence,
            "update_id_to": snapshot_sequence,
            "bids": snapshot.get("bids", []),
            "asks": snapshot.get("asks", []),
        }
        self.logger.info("Publishing bootstrap updates count=1")
        await self.output_handler.handle(
            exchange=self.exchange,
            market=self.market,
            symbol=self.symbol,
            raw_update=snapshot_event,
            raw_payload={
                "e": "snapshot",
                "s": self.symbol.upper(),
                "lastUpdateId": snapshot_sequence,
                "bids": snapshot.get("bids", []),
                "asks": snapshot.get("asks", []),
            },
        )
        self._last_sequence = snapshot_sequence
        self._awaiting_bridge = True

    async def _publish_live_delta(self, payload: Dict[str, Any]) -> None:
        update = self._extract_depth_update(payload)
        if update is None:
            return

        sequence = _safe_int(update.get("sequence"))
        if sequence is None:
            raise _BinanceResyncRequired("delta missing sequence")

        explicit_previous = _safe_int(payload.get("pu"))
        update_from = _safe_int(payload.get("U"), sequence)
        last_sequence = self._last_sequence
        if last_sequence is None:
            raise _BinanceResyncRequired("delta received before snapshot alignment")
        if sequence <= last_sequence:
            return

        if self._awaiting_bridge:
            if update_from is None or not (update_from <= last_sequence + 1 <= sequence):
                raise _BinanceResyncRequired(
                    f"bridge delta missing after snapshot last={last_sequence} U={update_from} u={sequence}"
                )
            update["prev_sequence"] = last_sequence
            self._awaiting_bridge = False
        elif explicit_previous is not None:
            if explicit_previous != last_sequence:
                raise _BinanceResyncRequired(
                    f"sequence discontinuity prev={explicit_previous} expected={last_sequence} seq={sequence}"
                )
        elif update_from is None or update_from > last_sequence + 1:
            raise _BinanceResyncRequired(
                f"update range discontinuity U={update_from} expected_max={last_sequence + 1} seq={sequence}"
            )

        await self.output_handler.handle(
            exchange=self.exchange,
            market=self.market,
            symbol=self.symbol,
            raw_update=update,
            raw_payload=payload,
        )
        self._last_sequence = sequence

    def _find_alignment_index(self, snapshot: Dict[str, Any], buffered_payloads: List[Dict[str, Any]]) -> Optional[int]:
        snapshot_sequence = snapshot["lastUpdateId"]
        target_sequence = snapshot_sequence + 1
        for index, payload in enumerate(buffered_payloads):
            update_from = _safe_int(payload.get("U"))
            sequence = _safe_int(payload.get("u"))
            if sequence is None:
                continue
            if sequence <= snapshot_sequence:
                continue
            if update_from is None:
                continue
            if update_from <= target_sequence <= sequence:
                return index
        return None

    def _prune_buffer(self, snapshot: Dict[str, Any], buffered_payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        snapshot_sequence = snapshot["lastUpdateId"]
        pruned = [
            payload
            for payload in buffered_payloads
            if (_safe_int(payload.get("u")) or 0) > snapshot_sequence
        ]
        if len(pruned) > 5000:
            return pruned[-5000:]
        return pruned

    async def _refresh_alignment_snapshot(
        self,
        snapshot: Dict[str, Any],
        buffered_payloads: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        oldest_update_from = None
        oldest_u = None
        if buffered_payloads:
            oldest_payload = buffered_payloads[0]
            oldest_update_from = _safe_int(oldest_payload.get("U"))
            oldest_u = _safe_int(oldest_payload.get("u"))

        refreshed_snapshot = await self._fetch_snapshot()
        self.logger.warning(
            "Refreshing alignment snapshot symbol=%s market=%s previous_lastUpdateId=%s "
            "refreshed_lastUpdateId=%s buffered=%s oldest_U=%s oldest_u=%s",
            self.symbol,
            self.market,
            snapshot["lastUpdateId"],
            refreshed_snapshot["lastUpdateId"],
            len(buffered_payloads),
            oldest_update_from,
            oldest_u,
        )
        return refreshed_snapshot

    def _extract_depth_update(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._is_depth_payload(payload):
            return None

        sequence = _safe_int(payload.get("u"))
        if sequence is None:
            self.logger.warning("Missing sequence in Binance payload")
            return None

        update_from = _safe_int(payload.get("U"), sequence)
        previous = _safe_int(payload.get("pu"))
        if previous is None and update_from is not None:
            previous = update_from - 1

        return {
            "event_type": "delta",
            "event_time_ms": _safe_int(payload.get("E"), int(time.time() * 1000)),
            "collector_receive_ts_ms": _safe_int(
                payload.get("_collector_receive_ts_ms"),
                int(time.time() * 1000),
            ),
            "sequence": sequence,
            "prev_sequence": previous,
            "update_id_from": update_from,
            "update_id_to": sequence,
            "bids": payload.get("b", []),
            "asks": payload.get("a", []),
        }

    def _is_depth_payload(self, payload: Dict[str, Any]) -> bool:
        if "result" in payload:
            return False
        if payload.get("e") != "depthUpdate":
            return False
        return str(payload.get("s", "")).upper() == self.symbol.upper()

    def _snapshot_url(self) -> str:
        if self.market in {"futures", "perp"}:
            return "https://fapi.binance.com/fapi/v1/depth"
        return "https://api.binance.com/api/v3/depth"

    def _check_event_freshness(self, payload: Dict[str, Any]) -> None:
        event_time_ms = _safe_int(payload.get("E"))
        if event_time_ms is None:
            return

        event_age_ms = int(time.time() * 1000) - event_time_ms
        if event_age_ms < 0:
            return

        if self.max_event_age_ms > 0 and event_age_ms > self.max_event_age_ms:
            raise _BinanceResyncRequired(
                f"stale delta age_ms={event_age_ms} limit_ms={self.max_event_age_ms}"
            )

        if (
            self.warn_event_age_ms > 0
            and event_age_ms > self.warn_event_age_ms
            and event_age_ms - self._last_event_age_warning_ms >= self.warn_event_age_ms
        ):
            self._last_event_age_warning_ms = event_age_ms
            self.logger.warning(
                "High source lag symbol=%s market=%s age_ms=%s warn_ms=%s",
                self.symbol,
                self.market,
                event_age_ms,
                self.warn_event_age_ms,
            )

    def _snapshot_is_too_old(
        self,
        snapshot: Dict[str, Any],
        buffered_payloads: List[Dict[str, Any]],
    ) -> bool:
        if not buffered_payloads:
            return False

        snapshot_sequence = snapshot["lastUpdateId"]
        target_sequence = snapshot_sequence + 1
        oldest_update_from = _safe_int(buffered_payloads[0].get("U"))
        if oldest_update_from is None:
            return False
        return oldest_update_from > target_sequence

    def _raise_if_alignment_stalled(
        self,
        snapshot: Dict[str, Any],
        buffered_payloads: List[Dict[str, Any]],
        snapshot_ready_at_monotonic: Optional[float],
    ) -> None:
        if snapshot_ready_at_monotonic is None or self.alignment_timeout_seconds <= 0:
            return
        elapsed_seconds = time.monotonic() - snapshot_ready_at_monotonic
        if elapsed_seconds < self.alignment_timeout_seconds:
            return

        snapshot_sequence = snapshot["lastUpdateId"]
        newest_u = None
        newest_event_time_ms = None
        if buffered_payloads:
            newest_payload = buffered_payloads[-1]
            newest_u = _safe_int(newest_payload.get("u"))
            newest_event_time_ms = _safe_int(newest_payload.get("E"))

        raise _BinanceResyncRequired(
            "snapshot alignment timeout "
            f"after={elapsed_seconds:.1f}s lastUpdateId={snapshot_sequence} "
            f"buffered={len(buffered_payloads)} newest_u={newest_u} "
            f"newest_event_time_ms={newest_event_time_ms}"
        )


def _load_json(raw_message: str) -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class _BinanceResyncRequired(Exception):
    pass
