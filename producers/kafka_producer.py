import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Sequence

from aiokafka import AIOKafkaProducer
try:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
except Exception:  # pragma: no cover - fallback for constrained aiokafka builds
    AIOKafkaAdminClient = None
    NewTopic = None


@dataclass
class TopicSettings:
    raw_template: str
    normalized: str
    scd: str

    @classmethod
    def from_dict(cls, config: Dict) -> "TopicSettings":
        topics = config.get("topics", {})
        if not isinstance(topics, dict):
            topics = {}

        legacy_topic = str(config.get("topic", "md.norm.depth.v1"))
        return cls(
            raw_template=str(topics.get("raw_template", "md.raw.{exchange}.{market}.depth.v1")),
            normalized=str(topics.get("normalized", legacy_topic)),
            scd=str(topics.get("scd", "md.scd.depth.levels.v1")),
        )


@dataclass
class TopicSpec:
    name: str
    num_partitions: int
    replication_factor: int


@dataclass
class TopicManagementSettings:
    enabled: bool = True
    num_partitions: int = 3
    replication_factor: int = 1
    timeout_ms: int = 10000
    topics_file: str = "config/topics.txt"

    @classmethod
    def from_dict(cls, config: Dict) -> "TopicManagementSettings":
        management = config.get("topic_management", {})
        if not isinstance(management, dict):
            management = {}
        return cls(
            enabled=bool(management.get("enabled", True)),
            num_partitions=int(management.get("num_partitions", 3)),
            replication_factor=int(management.get("replication_factor", 1)),
            timeout_ms=int(management.get("timeout_ms", 10000)),
            topics_file=str(management.get("topics_file", "config/topics.txt")),
        )


@dataclass
class KafkaSettings:
    bootstrap_servers: Sequence[str]
    topics: TopicSettings
    topic_management: TopicManagementSettings
    client_id: str = "market-depth-pipeline"
    linger_ms: int = 5
    compression_type: str = "gzip"
    max_send_retries: int = 3
    retry_backoff_seconds: float = 1.0

    @classmethod
    def from_dict(cls, config: Dict) -> "KafkaSettings":
        servers = config.get("bootstrap_servers", ["localhost:9092"])
        if isinstance(servers, str):
            servers = [servers]

        return cls(
            bootstrap_servers=servers,
            topics=TopicSettings.from_dict(config),
            topic_management=TopicManagementSettings.from_dict(config),
            client_id=config.get("client_id", "market-depth-pipeline"),
            linger_ms=int(config.get("linger_ms", 5)),
            compression_type=str(config.get("compression_type", "gzip")),
            max_send_retries=int(config.get("max_send_retries", 3)),
            retry_backoff_seconds=float(config.get("retry_backoff_seconds", 1.0)),
        )


class KafkaDepthProducer:
    def __init__(self, settings: KafkaSettings) -> None:
        self.settings = settings
        self.logger = logging.getLogger("producer.kafka")
        self._producer = None

    async def start(self) -> None:
        if self._producer is not None:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=list(self.settings.bootstrap_servers),
            client_id=self.settings.client_id,
            acks="all",
            enable_idempotence=True,
            linger_ms=self.settings.linger_ms,
            compression_type=self.settings.compression_type,
            value_serializer=self._serialize_event,
        )
        await self._producer.start()
        self.logger.info(
            "Kafka producer started bootstrap_servers=%s raw_template=%s normalized=%s scd=%s",
            ",".join(self.settings.bootstrap_servers),
            self.settings.topics.raw_template,
            self.settings.topics.normalized,
            self.settings.topics.scd,
        )

    async def stop(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        self._producer = None
        self.logger.info("Kafka producer stopped")

    async def ensure_topics(self, topic_specs: Iterable[TopicSpec]) -> None:
        if not self.settings.topic_management.enabled:
            return
        if AIOKafkaAdminClient is None or NewTopic is None:
            self.logger.warning(
                "Topic auto-management disabled because aiokafka admin client is unavailable."
            )
            return

        deduped_specs: Dict[str, TopicSpec] = {}
        for spec in topic_specs:
            topic_name = str(spec.name).strip()
            if not topic_name:
                continue
            deduped_specs[topic_name] = TopicSpec(
                name=topic_name,
                num_partitions=int(spec.num_partitions),
                replication_factor=int(spec.replication_factor),
            )
        if not deduped_specs:
            return

        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=list(self.settings.bootstrap_servers),
            client_id=f"{self.settings.client_id}-admin",
            request_timeout_ms=self.settings.topic_management.timeout_ms,
        )

        await admin_client.start()
        try:
            existing_topics = set(await admin_client.list_topics())
            new_topics = [
                NewTopic(
                    name=spec.name,
                    num_partitions=spec.num_partitions,
                    replication_factor=spec.replication_factor,
                )
                for spec in deduped_specs.values()
                if spec.name not in existing_topics
            ]
            if not new_topics:
                self.logger.info("All required topics already exist count=%s", len(deduped_specs))
                return

            await admin_client.create_topics(new_topics=new_topics, validate_only=False)
            self.logger.info(
                "Created missing topics count=%s topics=%s",
                len(new_topics),
                ",".join(sorted(topic.name for topic in new_topics)),
            )
        finally:
            await admin_client.close()

    async def send_raw_update(self, event: Dict) -> None:
        topic = self._build_raw_topic(event.get("exchange"), event.get("market"))
        await self._send(topic=topic, event=event)

    async def send_normalized_update(self, event: Dict) -> None:
        await self._send(topic=self.settings.topics.normalized, event=event)

    async def send_scd_update(self, event: Dict) -> None:
        await self._send(topic=self.settings.topics.scd, event=event)

    async def _send(self, topic: str, event: Dict) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")

        key = self._build_key(
            exchange=event.get("exchange"),
            market=event.get("market"),
            symbol=event.get("symbol"),
        )
        for attempt in range(1, self.settings.max_send_retries + 1):
            try:
                await self._producer.send_and_wait(
                    topic=topic,
                    key=key,
                    value=event,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception(
                    "Kafka publish failed attempt=%s stream=%s topic=%s",
                    attempt,
                    key.decode("utf-8"),
                    topic,
                )
                if attempt >= self.settings.max_send_retries:
                    raise
                await asyncio.sleep(self.settings.retry_backoff_seconds * attempt)

    def _build_raw_topic(self, exchange: Optional[str], market: Optional[str]) -> str:
        try:
            return self.settings.topics.raw_template.format(
                exchange=self._topic_part(exchange),
                market=self._topic_part(market),
            )
        except Exception:
            self.logger.exception(
                "Invalid raw topic template '%s'. Falling back to normalized topic.",
                self.settings.topics.raw_template,
            )
            return self.settings.topics.normalized

    @staticmethod
    def _build_key(exchange: Optional[str], market: Optional[str], symbol: Optional[str]) -> bytes:
        exchange_key = str(exchange or "unknown").strip().lower()
        market_key = str(market or "unknown").strip().lower()
        symbol_key = str(symbol or "unknown").strip().upper()
        return f"{exchange_key}|{market_key}|{symbol_key}".encode("utf-8")

    @staticmethod
    def _topic_part(value: Optional[str]) -> str:
        cleaned = str(value or "unknown").strip().lower()
        return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in cleaned)

    @staticmethod
    def _serialize_event(event: Dict) -> bytes:
        return json.dumps(event, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
