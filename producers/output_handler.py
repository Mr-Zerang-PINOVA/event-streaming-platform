import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple

from processors.data_processor import DataProcessor
from processors.scd_processor import SCDProcessor
from producers.kafka_producer import KafkaDepthProducer


class OutputHandler:
    def __init__(
        self,
        data_processor: DataProcessor,
        scd_processor: SCDProcessor,
        kafka_producer: KafkaDepthProducer,
        enable_scd_topic: bool = True,
        stream_queue_maxsize: int = 1000,
        queue_depth_log_interval_seconds: float = 10.0,
        queue_depth_warn_threshold: int = 800,
    ) -> None:
        self.data_processor = data_processor
        self.scd_processor = scd_processor
        self.kafka_producer = kafka_producer
        self.enable_scd_topic = bool(enable_scd_topic)
        self.stream_queue_maxsize = max(1, int(stream_queue_maxsize))
        self.queue_depth_log_interval_seconds = max(1.0, float(queue_depth_log_interval_seconds))
        self.queue_depth_warn_threshold = max(1, int(queue_depth_warn_threshold))
        self.logger = logging.getLogger("output.handler")
        self._stream_queues: Dict[str, asyncio.Queue[Optional[Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]]]] = {}
        self._stream_workers: Dict[str, asyncio.Task] = {}
        self._last_queue_depth_log_at: Dict[str, float] = {}
        self._stream_guard = asyncio.Lock()

    async def handle(
        self,
        exchange: str,
        market: str,
        symbol: str,
        raw_update: Dict,
        raw_payload: Dict,
    ) -> None:
        stream_key = f"{exchange}:{market}:{symbol}"
        queue = await self._get_stream_queue(stream_key)
        self._log_queue_depth(stream_key, queue, when_full=queue.full())
        await queue.put((exchange, market, symbol, raw_update, raw_payload))
        self._log_queue_depth(stream_key, queue)

    async def stop(self) -> None:
        async with self._stream_guard:
            queues = list(self._stream_queues.values())
            workers = list(self._stream_workers.values())

        for queue in queues:
            await queue.put(None)
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

    async def _get_stream_queue(
        self,
        stream_key: str,
    ) -> asyncio.Queue[Optional[Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]]]:
        async with self._stream_guard:
            queue = self._stream_queues.get(stream_key)
            if queue is None:
                queue = asyncio.Queue(maxsize=self.stream_queue_maxsize)
                self._stream_queues[stream_key] = queue
                self._stream_workers[stream_key] = asyncio.create_task(
                    self._run_stream_worker(stream_key, queue),
                    name=f"output:{stream_key}",
                )
            return queue

    async def _run_stream_worker(
        self,
        stream_key: str,
        queue: asyncio.Queue[Optional[Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]]],
    ) -> None:
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return

                exchange, market, symbol, raw_update, raw_payload = item
                await self._publish_stream_event(
                    stream_key=stream_key,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    raw_update=raw_update,
                    raw_payload=raw_payload,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Stream publish failed stream=%s", stream_key)
            finally:
                queue.task_done()
                self._log_queue_depth(stream_key, queue)

    async def _publish_stream_event(
        self,
        stream_key: str,
        exchange: str,
        market: str,
        symbol: str,
        raw_update: Dict[str, Any],
        raw_payload: Dict[str, Any],
    ) -> None:
        collector_receive_ts_ms = int(raw_update.get("collector_receive_ts_ms", int(time.time() * 1000)))

        raw_event = self._build_raw_event(
            exchange=exchange,
            market=market,
            symbol=symbol,
            raw_update=raw_update,
            raw_payload=raw_payload,
            collector_receive_ts_ms=collector_receive_ts_ms,
        )
        await self.kafka_producer.send_raw_update(raw_event)

        normalized = self.data_processor.process(
            exchange,
            market,
            symbol,
            {**raw_update, "collector_receive_ts_ms": collector_receive_ts_ms},
        )
        if normalized is None:
            return

        await self.kafka_producer.send_normalized_update(normalized)

        scd_events = []
        if self.enable_scd_topic:
            scd_events = self.scd_processor.process(normalized)
            for scd_event in scd_events:
                await self.kafka_producer.send_scd_update(scd_event)

        self.logger.debug(
            "Published layered events stream=%s event_type=%s sequence=%s scd_count=%s",
            stream_key,
            normalized.get("event_type"),
            normalized.get("sequence"),
            len(scd_events),
        )

    def _log_queue_depth(
        self,
        stream_key: str,
        queue: asyncio.Queue[Optional[Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]]],
        when_full: bool = False,
    ) -> None:
        qsize = queue.qsize()
        if qsize <= 0:
            return

        now = time.monotonic()
        last_logged_at = self._last_queue_depth_log_at.get(stream_key, 0.0)
        should_log = False
        level = logging.INFO

        if when_full:
            should_log = True
            level = logging.WARNING
        elif qsize >= self.queue_depth_warn_threshold:
            should_log = True
            level = logging.WARNING
        elif now - last_logged_at >= self.queue_depth_log_interval_seconds:
            should_log = True

        if not should_log:
            return

        self._last_queue_depth_log_at[stream_key] = now
        self.logger.log(
            level,
            "Stream queue depth stream=%s depth=%s maxsize=%s full=%s",
            stream_key,
            qsize,
            queue.maxsize,
            queue.full(),
        )

    @staticmethod
    def _build_raw_event(
        exchange: str,
        market: str,
        symbol: str,
        raw_update: Dict,
        raw_payload: Dict,
        collector_receive_ts_ms: int,
    ) -> Dict:
        return {
            "schema_version": 1,
            "layer": "raw",
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "event_type": str(raw_update.get("event_type", "delta")).lower(),
            "event_time_ms": int(raw_update.get("event_time_ms", int(time.time() * 1000))),
            "collector_receive_ts_ms": collector_receive_ts_ms,
            "ingest_time_ms": int(time.time() * 1000),
            "sequence": raw_update.get("sequence"),
            "prev_sequence": raw_update.get("prev_sequence"),
            "snapshot_last_update_id": raw_update.get("snapshot_last_update_id"),
            "update_id_from": raw_update.get("update_id_from"),
            "update_id_to": raw_update.get("update_id_to"),
            "bids": raw_update.get("bids", []),
            "asks": raw_update.get("asks", []),
            "source_payload": raw_payload,
        }
