import hashlib
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple


class SCDProcessor:
    def __init__(self) -> None:
        self.logger = logging.getLogger("processor.scd")
        self._state: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}

    def process(self, normalized_event: Dict[str, Any]) -> List[Dict[str, Any]]:
        exchange = str(normalized_event.get("exchange", ""))
        market = str(normalized_event.get("market", ""))
        symbol = str(normalized_event.get("symbol", ""))
        event_type = str(normalized_event.get("event_type", "delta")).lower()
        event_time_ms = _safe_int(normalized_event.get("event_time_ms"), int(time.time() * 1000))
        sequence = _safe_int(normalized_event.get("sequence"))
        prev_sequence = _safe_int(normalized_event.get("prev_sequence"))
        ingest_time_ms = int(time.time() * 1000)

        stream_key = f"{exchange}:{market}:{symbol}"
        stream_state = self._state.setdefault(stream_key, {"bids": {}, "asks": {}})

        scd_events: List[Dict[str, Any]] = []
        for side in ("bids", "asks"):
            side_state = stream_state[side]
            levels = normalized_event.get(side, [])
            if event_type == "snapshot":
                events = self._apply_snapshot_side(
                    side_state=side_state,
                    side=side,
                    levels=levels,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    event_type=event_type,
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    sequence=sequence,
                    prev_sequence=prev_sequence,
                )
            else:
                events = self._apply_delta_side(
                    side_state=side_state,
                    side=side,
                    levels=levels,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    event_type=event_type,
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    sequence=sequence,
                    prev_sequence=prev_sequence,
                )
            scd_events.extend(events)

        if scd_events:
            self.logger.debug(
                "Generated scd events stream=%s count=%s source_sequence=%s",
                stream_key,
                len(scd_events),
                sequence,
            )
        return scd_events

    def _apply_snapshot_side(
        self,
        side_state: Dict[str, Dict[str, Any]],
        side: str,
        levels: Any,
        exchange: str,
        market: str,
        symbol: str,
        event_type: str,
        event_time_ms: int,
        ingest_time_ms: int,
        sequence: Optional[int],
        prev_sequence: Optional[int],
    ) -> List[Dict[str, Any]]:
        normalized_incoming: Dict[str, str] = {}
        for price, qty in self._normalize_levels(levels):
            normalized_incoming[price] = qty

        events: List[Dict[str, Any]] = []

        for price, existing_entry in list(side_state.items()):
            if price in normalized_incoming:
                continue
            events.append(
                self._build_close_event(
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    side=side,
                    price=price,
                    entry=existing_entry,
                    event_type=event_type,
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    sequence=sequence,
                    prev_sequence=prev_sequence,
                    reason="snapshot_replace",
                )
            )
            del side_state[price]

        for price, qty in normalized_incoming.items():
            if _is_zero_qty(qty):
                if price in side_state:
                    events.append(
                        self._build_close_event(
                            exchange=exchange,
                            market=market,
                            symbol=symbol,
                            side=side,
                            price=price,
                            entry=side_state[price],
                            event_type=event_type,
                            event_time_ms=event_time_ms,
                            ingest_time_ms=ingest_time_ms,
                            sequence=sequence,
                            prev_sequence=prev_sequence,
                            reason="snapshot_zero_qty",
                        )
                    )
                    del side_state[price]
                continue

            existing_entry = side_state.get(price)
            if existing_entry is None:
                new_entry = self._open_entry(qty=qty, event_time_ms=event_time_ms, sequence=sequence)
                side_state[price] = new_entry
                events.append(
                    self._build_open_event(
                        exchange=exchange,
                        market=market,
                        symbol=symbol,
                        side=side,
                        price=price,
                        entry=new_entry,
                        event_type=event_type,
                        event_time_ms=event_time_ms,
                        ingest_time_ms=ingest_time_ms,
                        sequence=sequence,
                        prev_sequence=prev_sequence,
                        change_type="open",
                    )
                )
                continue

            if existing_entry["qty"] != qty:
                events.append(
                    self._build_close_event(
                        exchange=exchange,
                        market=market,
                        symbol=symbol,
                        side=side,
                        price=price,
                        entry=existing_entry,
                        event_type=event_type,
                        event_time_ms=event_time_ms,
                        ingest_time_ms=ingest_time_ms,
                        sequence=sequence,
                        prev_sequence=prev_sequence,
                        reason="update",
                    )
                )
                new_entry = self._open_entry(qty=qty, event_time_ms=event_time_ms, sequence=sequence)
                side_state[price] = new_entry
                events.append(
                    self._build_open_event(
                        exchange=exchange,
                        market=market,
                        symbol=symbol,
                        side=side,
                        price=price,
                        entry=new_entry,
                        event_type=event_type,
                        event_time_ms=event_time_ms,
                        ingest_time_ms=ingest_time_ms,
                        sequence=sequence,
                        prev_sequence=prev_sequence,
                        change_type="update",
                    )
                )

        return events

    def _apply_delta_side(
        self,
        side_state: Dict[str, Dict[str, Any]],
        side: str,
        levels: Any,
        exchange: str,
        market: str,
        symbol: str,
        event_type: str,
        event_time_ms: int,
        ingest_time_ms: int,
        sequence: Optional[int],
        prev_sequence: Optional[int],
    ) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for price, qty in self._normalize_levels(levels):
            existing_entry = side_state.get(price)
            if _is_zero_qty(qty):
                if existing_entry is None:
                    continue
                events.append(
                    self._build_close_event(
                        exchange=exchange,
                        market=market,
                        symbol=symbol,
                        side=side,
                        price=price,
                        entry=existing_entry,
                        event_type=event_type,
                        event_time_ms=event_time_ms,
                        ingest_time_ms=ingest_time_ms,
                        sequence=sequence,
                        prev_sequence=prev_sequence,
                        reason="delete",
                    )
                )
                del side_state[price]
                continue

            if existing_entry is None:
                new_entry = self._open_entry(qty=qty, event_time_ms=event_time_ms, sequence=sequence)
                side_state[price] = new_entry
                events.append(
                    self._build_open_event(
                        exchange=exchange,
                        market=market,
                        symbol=symbol,
                        side=side,
                        price=price,
                        entry=new_entry,
                        event_type=event_type,
                        event_time_ms=event_time_ms,
                        ingest_time_ms=ingest_time_ms,
                        sequence=sequence,
                        prev_sequence=prev_sequence,
                        change_type="open",
                    )
                )
                continue

            if existing_entry["qty"] == qty:
                continue

            events.append(
                self._build_close_event(
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    side=side,
                    price=price,
                    entry=existing_entry,
                    event_type=event_type,
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    sequence=sequence,
                    prev_sequence=prev_sequence,
                    reason="update",
                )
            )
            new_entry = self._open_entry(qty=qty, event_time_ms=event_time_ms, sequence=sequence)
            side_state[price] = new_entry
            events.append(
                self._build_open_event(
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    side=side,
                    price=price,
                    entry=new_entry,
                    event_type=event_type,
                    event_time_ms=event_time_ms,
                    ingest_time_ms=ingest_time_ms,
                    sequence=sequence,
                    prev_sequence=prev_sequence,
                    change_type="update",
                )
            )
        return events

    @staticmethod
    def _open_entry(qty: str, event_time_ms: int, sequence: Optional[int]) -> Dict[str, Any]:
        return {
            "qty": qty,
            "valid_from_ms": event_time_ms,
            "open_sequence": sequence,
        }

    def _build_open_event(
        self,
        exchange: str,
        market: str,
        symbol: str,
        side: str,
        price: str,
        entry: Dict[str, Any],
        event_type: str,
        event_time_ms: int,
        ingest_time_ms: int,
        sequence: Optional[int],
        prev_sequence: Optional[int],
        change_type: str,
    ) -> Dict[str, Any]:
        payload = {
            "schema_version": 1,
            "layer": "scd",
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "side": side,
            "price": price,
            "qty": entry["qty"],
            "valid_from_ms": entry["valid_from_ms"],
            "valid_to_ms": None,
            "is_current": True,
            "change_type": change_type,
            "close_reason": None,
            "source_event_type": event_type,
            "source_sequence": sequence,
            "source_prev_sequence": prev_sequence,
            "open_sequence": entry.get("open_sequence"),
            "close_sequence": None,
            "event_time_ms": event_time_ms,
            "ingest_time_ms": ingest_time_ms,
        }
        payload["scd_event_id"] = _make_scd_event_id(payload)
        return payload

    def _build_close_event(
        self,
        exchange: str,
        market: str,
        symbol: str,
        side: str,
        price: str,
        entry: Dict[str, Any],
        event_type: str,
        event_time_ms: int,
        ingest_time_ms: int,
        sequence: Optional[int],
        prev_sequence: Optional[int],
        reason: str,
    ) -> Dict[str, Any]:
        payload = {
            "schema_version": 1,
            "layer": "scd",
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "side": side,
            "price": price,
            "qty": entry["qty"],
            "valid_from_ms": entry.get("valid_from_ms"),
            "valid_to_ms": event_time_ms,
            "is_current": False,
            "change_type": "close",
            "close_reason": reason,
            "source_event_type": event_type,
            "source_sequence": sequence,
            "source_prev_sequence": prev_sequence,
            "open_sequence": entry.get("open_sequence"),
            "close_sequence": sequence,
            "event_time_ms": event_time_ms,
            "ingest_time_ms": ingest_time_ms,
        }
        payload["scd_event_id"] = _make_scd_event_id(payload)
        return payload

    @staticmethod
    def _normalize_levels(levels: Any) -> List[Tuple[str, str]]:
        if not isinstance(levels, list):
            return []

        normalized: List[Tuple[str, str]] = []
        for level in levels:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                price = level[0]
                qty = level[1]
            elif isinstance(level, dict):
                price = level.get("price", level.get("px"))
                qty = level.get("size", level.get("qty", level.get("amount")))
            else:
                continue

            p = _normalize_decimal(price)
            q = _normalize_decimal(qty)
            if p is None or q is None:
                continue
            if Decimal(p) <= 0:
                continue
            normalized.append((p, q))
        return normalized


def _normalize_decimal(value: Any) -> Optional[str]:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(parsed, "f")


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_zero_qty(value: str) -> bool:
    try:
        return Decimal(value) == 0
    except (InvalidOperation, TypeError, ValueError):
        return False


def _make_scd_event_id(payload: Dict[str, Any]) -> str:
    base = (
        f"{payload.get('exchange')}|{payload.get('market')}|{payload.get('symbol')}|"
        f"{payload.get('side')}|{payload.get('price')}|{payload.get('qty')}|"
        f"{payload.get('valid_from_ms')}|{payload.get('valid_to_ms')}|"
        f"{payload.get('change_type')}|{payload.get('source_sequence')}|"
        f"{payload.get('close_reason')}"
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()
