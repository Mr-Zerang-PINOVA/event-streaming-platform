import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from contracts.event_ids import make_normalized_event_id


class DataProcessor:
    def __init__(self, max_levels: int = 50) -> None:
        self.max_levels = max_levels
        self.logger = logging.getLogger("processor.data")
        self._last_sequence_by_stream: Dict[str, int] = {}

    def process(
        self,
        exchange: str,
        market: str,
        symbol: str,
        raw_update: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        stream_key = f"{exchange}:{market}:{symbol}"
        event_time_ms = _safe_int(raw_update.get("event_time_ms"), int(time.time() * 1000))
        collector_receive_ts_ms = _safe_int(raw_update.get("collector_receive_ts_ms"), int(time.time() * 1000))
        sequence = _safe_int(raw_update.get("sequence"))
        prev_sequence = _safe_int(raw_update.get("prev_sequence"))
        update_id_from = _safe_int(raw_update.get("update_id_from"), sequence)
        update_id_to = _safe_int(raw_update.get("update_id_to"), sequence)
        snapshot_last_update_id = _safe_int(raw_update.get("snapshot_last_update_id"))
        event_type = str(raw_update.get("event_type", "delta")).lower()
        raw_event_id = raw_update.get("raw_event_id") or raw_update.get("event_id")

        if not self._validate_sequence(stream_key, sequence, prev_sequence, event_type):
            return None

        bids = self._normalize_levels(raw_update.get("bids", []), side="bids")
        asks = self._normalize_levels(raw_update.get("asks", []), side="asks")
        if not bids and not asks:
            self.logger.debug("Skipping empty orderbook update stream=%s", stream_key)
            return None

        normalized_event_id = make_normalized_event_id(
            raw_event_id=str(raw_event_id) if raw_event_id else None,
            exchange=exchange,
            market=market,
            symbol=symbol,
            event_type=event_type,
            event_time_ms=event_time_ms,
            sequence=sequence,
            prev_sequence=prev_sequence,
            update_id_from=update_id_from,
            update_id_to=update_id_to,
            snapshot_last_update_id=snapshot_last_update_id,
            bids=bids,
            asks=asks,
        )
        return {
            "schema_version": 1,
            "contract_version": 1,
            "layer": "normalized",
            "event_id": normalized_event_id,
            "normalized_event_id": normalized_event_id,
            "raw_event_id": str(raw_event_id) if raw_event_id else None,
            "source_event_id": str(raw_event_id) if raw_event_id else None,
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "event_type": event_type,
            "event_time_ms": event_time_ms,
            "collector_receive_ts_ms": collector_receive_ts_ms,
            "ingest_time_ms": int(time.time() * 1000),
            "sequence": sequence,
            "prev_sequence": prev_sequence,
            "update_id_from": update_id_from,
            "update_id_to": update_id_to,
            "snapshot_last_update_id": snapshot_last_update_id,
            "bids": bids,
            "asks": asks,
        }

    def _validate_sequence(
        self,
        stream_key: str,
        sequence: Optional[int],
        prev_sequence: Optional[int],
        event_type: str,
    ) -> bool:
        if sequence is None:
            return True

        if event_type == "snapshot":
            self._last_sequence_by_stream[stream_key] = sequence
            self.logger.info("Sequence baseline reset stream=%s snapshot_sequence=%s", stream_key, sequence)
            return True

        last_sequence = self._last_sequence_by_stream.get(stream_key)
        if last_sequence is not None and sequence <= last_sequence:
            self.logger.warning(
                "Out-of-order or duplicate event skipped stream=%s last_sequence=%s current_sequence=%s",
                stream_key,
                last_sequence,
                sequence,
            )
            return False

        if last_sequence is not None and prev_sequence is not None and prev_sequence != last_sequence:
            self.logger.warning(
                "Sequence gap detected stream=%s expected_prev=%s received_prev=%s received_seq=%s",
                stream_key,
                last_sequence,
                prev_sequence,
                sequence,
            )

        self._last_sequence_by_stream[stream_key] = sequence
        return True

    def _normalize_levels(self, levels: Any, side: str) -> List[List[str]]:
        if not isinstance(levels, list):
            return []

        normalized: List[List[str]] = []
        for level in levels:
            price, quantity = self._extract_level(level)
            if price is None or quantity is None:
                continue

            try:
                price_value = Decimal(str(price))
                quantity_value = Decimal(str(quantity))
            except (InvalidOperation, ValueError, TypeError):
                continue

            if price_value <= 0 or quantity_value < 0:
                continue

            normalized.append([format(price_value, "f"), format(quantity_value, "f")])

        reverse_sort = side == "bids"
        normalized.sort(key=lambda item: Decimal(item[0]), reverse=reverse_sort)

        if self.max_levels > 0:
            return normalized[: self.max_levels]
        return normalized

    @staticmethod
    def _extract_level(level: Any) -> Tuple[Optional[Any], Optional[Any]]:
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return level[0], level[1]
        if isinstance(level, dict):
            price = level.get("price", level.get("px"))
            quantity = level.get("size", level.get("qty", level.get("amount")))
            return price, quantity
        return None, None


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
