import json
import time
from typing import Any, Dict, List

from .base_collector import BaseCollector


class OKXCollector(BaseCollector):
    def __init__(self, channel: str = "books", **kwargs) -> None:
        super().__init__(**kwargs)
        self.channel = channel

    async def subscribe(self, websocket_client: Any) -> None:
        subscribe_message = {
            "op": "subscribe",
            "args": [{"channel": self.channel, "instId": self.symbol}],
        }
        await websocket_client.send(json.dumps(subscribe_message))
        self.logger.info("Subscribed channel=%s instId=%s", self.channel, self.symbol)

    def extract_depth_updates(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if payload.get("event") == "subscribe":
            return []
        if payload.get("event") == "error":
            self.logger.error("OKX subscription error payload=%s", payload)
            return []

        arg = payload.get("arg")
        if not isinstance(arg, dict):
            return []
        if str(arg.get("channel", "")) != self.channel:
            return []
        if str(arg.get("instId", "")).upper() != self.symbol.upper():
            return []

        data = payload.get("data")
        if not isinstance(data, list):
            return []

        action = str(payload.get("action", "update")).lower()
        event_type = "snapshot" if action == "snapshot" else "delta"

        updates: List[Dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            sequence = _safe_int(item.get("seqId"))
            prev_sequence = _safe_int(item.get("prevSeqId"))
            if event_type == "snapshot":
                update_id_from = sequence
                update_id_to = sequence
            else:
                # OKX deltas bridge snapshots via prevSeqId -> seqId, not via contiguous +1 ranges.
                update_id_from = prev_sequence if prev_sequence is not None and prev_sequence >= 0 else sequence
                update_id_to = sequence
            updates.append(
                {
                    "event_type": event_type,
                    "event_time_ms": _safe_int(item.get("ts"), int(time.time() * 1000)),
                    "sequence": sequence,
                    "prev_sequence": prev_sequence,
                    "update_id_from": update_id_from,
                    "update_id_to": update_id_to,
                    "bids": item.get("bids", []),
                    "asks": item.get("asks", []),
                }
            )

        return updates


def _safe_int(value: Any, default: int = None) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
