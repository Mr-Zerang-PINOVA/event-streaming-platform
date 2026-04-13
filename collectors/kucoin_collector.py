import json
import time
from typing import Any, Dict, List

from .base_collector import BaseCollector


class KucoinCollector(BaseCollector):
    def __init__(self, topic: str = "/market/level2", **kwargs) -> None:
        super().__init__(**kwargs)
        self.topic = topic

    async def subscribe(self, websocket_client: Any) -> None:
        full_topic = f"{self.topic}:{self.symbol}"
        subscribe_message = {
            "id": str(int(time.time() * 1000)),
            "type": "subscribe",
            "topic": full_topic,
            "privateChannel": False,
            "response": True,
        }
        await websocket_client.send(json.dumps(subscribe_message))
        self.logger.info("Subscribed topic=%s", full_topic)

    def extract_depth_updates(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        message_type = payload.get("type")
        if message_type in {"welcome", "ack", "pong"}:
            return []
        if message_type != "message":
            return []

        expected_topic = f"{self.topic}:{self.symbol}"
        if payload.get("topic") != expected_topic:
            return []

        data = payload.get("data")
        if not isinstance(data, dict):
            return []

        changes = data.get("changes", {})
        if not isinstance(changes, dict):
            changes = {}

        sequence = _safe_int(data.get("sequenceEnd"), _safe_int(data.get("sequence")))
        sequence_start = _safe_int(data.get("sequenceStart"))
        prev_sequence = sequence_start - 1 if sequence_start is not None else None

        return [
            {
                "event_type": "delta",
                "event_time_ms": _safe_int(data.get("time"), int(time.time() * 1000)),
                "sequence": sequence,
                "prev_sequence": prev_sequence,
                "bids": changes.get("bids", []),
                "asks": changes.get("asks", []),
            }
        ]


def _safe_int(value: Any, default: int = None) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

