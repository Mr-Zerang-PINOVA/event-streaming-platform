import json
import time
from typing import Any, Dict, List

from .base_collector import BaseCollector


class BybitCollector(BaseCollector):
    def __init__(self, channel: str = "orderbook.50", **kwargs) -> None:
        super().__init__(**kwargs)
        self.channel = channel

    async def subscribe(self, websocket_client: Any) -> None:
        topic = f"{self.channel}.{self.symbol}"
        subscribe_message = {"op": "subscribe", "args": [topic]}
        await websocket_client.send(json.dumps(subscribe_message))
        self.logger.info("Subscribed topic=%s", topic)

    def extract_depth_updates(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if payload.get("op") == "subscribe":
            return []
        if payload.get("ret_msg") == "pong":
            return []

        topic = str(payload.get("topic", ""))
        if not topic.startswith(self.channel):
            return []

        data = payload.get("data")
        if isinstance(data, list):
            if not data:
                return []
            data = data[0]
        if not isinstance(data, dict):
            return []

        message_symbol = str(data.get("s", topic.split(".")[-1])).upper()
        if message_symbol != self.symbol.upper():
            return []

        event_type = "snapshot" if str(payload.get("type", "")).lower() == "snapshot" else "delta"
        sequence = _safe_int(data.get("u"), _safe_int(data.get("seq")))
        event_time_ms = _safe_int(data.get("cts"), _safe_int(payload.get("ts"), int(time.time() * 1000)))

        return [
            {
                "event_type": event_type,
                "event_time_ms": event_time_ms,
                "sequence": sequence,
                "prev_sequence": None,
                "bids": data.get("b", []),
                "asks": data.get("a", []),
            }
        ]


def _safe_int(value: Any, default: int = None) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

