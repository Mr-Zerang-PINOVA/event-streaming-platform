import hashlib
import json
from typing import Any, Dict, Optional


_VOLATILE_KEYS = {
    "collector_receive_ts_ms",
    "ingest_time_ms",
    "_collector_receive_ts_ms",
}


def make_raw_event_id(
    *,
    exchange: str,
    market: str,
    symbol: str,
    event_type: str,
    event_time_ms: Optional[int],
    sequence: Optional[int],
    prev_sequence: Optional[int],
    update_id_from: Optional[int],
    update_id_to: Optional[int],
    snapshot_last_update_id: Optional[int],
    bids: Any,
    asks: Any,
    source_payload: Optional[Dict[str, Any]] = None,
) -> str:
    return _stable_hash(
        "raw.v1",
        {
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "event_type": event_type,
            "event_time_ms": event_time_ms,
            "sequence": sequence,
            "prev_sequence": prev_sequence,
            "update_id_from": update_id_from,
            "update_id_to": update_id_to,
            "snapshot_last_update_id": snapshot_last_update_id,
            "bids": bids,
            "asks": asks,
            "source_payload": source_payload or {},
        },
    )


def make_normalized_event_id(
    *,
    raw_event_id: Optional[str],
    exchange: str,
    market: str,
    symbol: str,
    event_type: str,
    event_time_ms: Optional[int],
    sequence: Optional[int],
    prev_sequence: Optional[int],
    update_id_from: Optional[int],
    update_id_to: Optional[int],
    snapshot_last_update_id: Optional[int],
    bids: Any,
    asks: Any,
) -> str:
    return _stable_hash(
        "normalized.v1",
        {
            "raw_event_id": raw_event_id,
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "event_type": event_type,
            "event_time_ms": event_time_ms,
            "sequence": sequence,
            "prev_sequence": prev_sequence,
            "update_id_from": update_id_from,
            "update_id_to": update_id_to,
            "snapshot_last_update_id": snapshot_last_update_id,
            "bids": bids,
            "asks": asks,
        },
    )


def make_scd_row_id(
    *,
    exchange: str,
    market: str,
    symbol: str,
    side: str,
    price: Any,
    qty: Any,
    valid_from_ms: Optional[int],
    valid_to_ms: Optional[int],
    source_event_id: Optional[str],
    change_type: str,
    close_reason: Optional[str] = None,
    source_sequence: Optional[int] = None,
    open_sequence: Optional[int] = None,
    close_sequence: Optional[int] = None,
) -> str:
    return _stable_hash(
        "scd.v1",
        {
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "side": side,
            "price": price,
            "qty": qty,
            "valid_from_ms": valid_from_ms,
            "valid_to_ms": valid_to_ms,
            "source_event_id": source_event_id,
            "change_type": change_type,
            "close_reason": close_reason,
            "source_sequence": source_sequence,
            "open_sequence": open_sequence,
            "close_sequence": close_sequence,
        },
    )


def resolve_event_id(event: Dict[str, Any]) -> Optional[str]:
    for field in (
        "event_id",
        "normalized_event_id",
        "raw_event_id",
        "scd_row_id",
        "scd_event_id",
    ):
        value = event.get(field)
        if value:
            return str(value)
    return None


def _stable_hash(namespace: str, payload: Dict[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(namespace.encode("utf-8"))
    digest.update(b"\x1f")
    digest.update(_canonical_json(payload).encode("utf-8"))
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _strip_volatile_fields(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _strip_volatile_fields(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key in sorted(value):
            key_str = str(key)
            if key_str.startswith("_") or key_str in _VOLATILE_KEYS:
                continue
            cleaned[key_str] = _strip_volatile_fields(value[key])
        return cleaned
    if isinstance(value, (list, tuple)):
        return [_strip_volatile_fields(item) for item in value]
    return value
