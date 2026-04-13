import abc
import asyncio
import json
import logging
import random
import socket
from typing import Any, Dict, List

import websockets


class BaseCollector(abc.ABC):
    def __init__(
        self,
        exchange: str,
        ws_url: str,
        symbol: str,
        market: str,
        output_handler,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        ping_interval_seconds: float = 20.0,
        ping_timeout_seconds: float = 20.0,
        **_,
    ) -> None:
        self.exchange = exchange
        self.ws_url = ws_url
        self.symbol = symbol
        self.market = market
        self.output_handler = output_handler
        self.reconnect_base_seconds = reconnect_base_seconds
        self.reconnect_max_seconds = reconnect_max_seconds
        self.ping_interval_seconds = ping_interval_seconds
        self.ping_timeout_seconds = ping_timeout_seconds
        self.logger = logging.getLogger(f"collector.{exchange}.{market}.{symbol}")
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        backoff = self.reconnect_base_seconds
        while not self._stop_event.is_set():
            try:
                self.logger.info("Connecting ws_url=%s", self.ws_url)
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=self.ping_interval_seconds,
                    ping_timeout=self.ping_timeout_seconds,
                    max_queue=4096,
                ) as websocket_client:
                    self.logger.info("Connected. Subscribing symbol=%s", self.symbol)
                    await self.subscribe(websocket_client)
                    bootstrap_updates = await self.bootstrap_updates()
                    if bootstrap_updates:
                        self.logger.info("Publishing bootstrap updates count=%s", len(bootstrap_updates))
                        for update in bootstrap_updates:
                            await self.output_handler.handle(
                                exchange=self.exchange,
                                market=self.market,
                                symbol=self.symbol,
                                raw_update=update,
                                raw_payload={"source": "bootstrap"},
                            )
                    backoff = self.reconnect_base_seconds
                    async for raw_message in websocket_client:
                        if self._stop_event.is_set():
                            break
                        await self._handle_message(raw_message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason, detail = self._summarize_connection_error(exc)
                self.logger.error(
                    "Collector connection failed stream=%s ws_url=%s reason=%s detail=%s. Reconnect is scheduled.",
                    f"{self.exchange}:{self.market}:{self.symbol}",
                    self.ws_url,
                    reason,
                    detail,
                )
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug("Collector connection traceback:", exc_info=exc)

            if self._stop_event.is_set():
                break

            sleep_seconds = min(backoff, self.reconnect_max_seconds)
            jitter = random.uniform(0.0, sleep_seconds * 0.2)
            sleep_seconds += jitter
            self.logger.warning("Reconnecting in %.2f seconds", sleep_seconds)
            await asyncio.sleep(sleep_seconds)
            backoff = min(backoff * 2, self.reconnect_max_seconds)

        self.logger.info("Collector stopped")

    async def _handle_message(self, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            self.logger.debug("Skipping non-JSON message")
            return

        updates = self.extract_depth_updates(payload)
        if not updates:
            return

        for update in updates:
            await self.output_handler.handle(
                exchange=self.exchange,
                market=self.market,
                symbol=self.symbol,
                raw_update=update,
                raw_payload=payload,
            )

    async def bootstrap_updates(self) -> List[Dict[str, Any]]:
        return []

    @staticmethod
    def _summarize_connection_error(exc: Exception) -> tuple[str, str]:
        chain = BaseCollector._exception_chain(exc)

        for current in chain:
            if isinstance(current, asyncio.TimeoutError) or isinstance(current, TimeoutError):
                return "connect_timeout", str(current) or "connection attempt timed out"
            if isinstance(current, socket.gaierror):
                return "dns_resolution_failed", str(current)
            if isinstance(current, ConnectionRefusedError):
                return "connection_refused", str(current)
            if isinstance(current, OSError):
                errno = getattr(current, "errno", None)
                if errno == 101:
                    return "network_unreachable", str(current)
                if errno == 113:
                    return "host_unreachable", str(current)
                if errno == 111:
                    return "connection_refused", str(current)
                if errno == 110:
                    return "connect_timeout", str(current)

        root = chain[-1] if chain else exc
        detail = str(root) or root.__class__.__name__
        return root.__class__.__name__.lower(), detail

    @staticmethod
    def _exception_chain(exc: BaseException) -> list[BaseException]:
        chain: list[BaseException] = []
        current: BaseException | None = exc
        while current is not None:
            chain.append(current)
            next_exc = current.__cause__ or current.__context__
            if next_exc is current:
                break
            current = next_exc
        return chain

    @abc.abstractmethod
    async def subscribe(self, websocket_client: Any) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def extract_depth_updates(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError
