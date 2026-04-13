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

        updates: List[Dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            action = str(item.get("action", "update")).lower()
            event_type = "snapshot" if action == "snapshot" else "delta"
            updates.append(
                {
                    "event_type": event_type,
                    "event_time_ms": _safe_int(item.get("ts"), int(time.time() * 1000)),
                    "sequence": _safe_int(item.get("seqId")),
                    "prev_sequence": _safe_int(item.get("prevSeqId")),
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

