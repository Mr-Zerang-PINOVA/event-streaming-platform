import argparse
import asyncio
import logging
import signal
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

from collectors.factory import build_collectors
from processors.data_processor import DataProcessor
from processors.scd_processor import SCDProcessor
from producers.kafka_producer import KafkaDepthProducer, KafkaSettings, TopicSpec
from producers.output_handler import OutputHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-exchange market depth pipeline")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to pipeline YAML configuration",
    )
    return parser.parse_args()


def load_config(config_path: Path) -> Dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}
    if not isinstance(config, dict):
        raise ValueError("Config root must be a YAML object")
    return config


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def create_collectors(config: Dict, output_handler: OutputHandler) -> List:
    connection_entries = _load_connection_entries(config)
    if not connection_entries:
        raise ValueError("Config must include non-empty 'connections' list or 'exchanges' object")

    pipeline_config = config.get("pipeline", {})
    reconnect_base_seconds = float(pipeline_config.get("reconnect_base_seconds", 1.0))
    reconnect_max_seconds = float(pipeline_config.get("reconnect_max_seconds", 30.0))

    collectors = []
    for exchange_name, exchange_config in connection_entries:
        if not isinstance(exchange_config, dict):
            logging.getLogger("main").error("Skipping invalid connection exchange=%s", exchange_name)
            continue
        if exchange_config.get("enabled", True) is False:
            logging.getLogger("main").info(
                "Connection disabled exchange=%s market=%s",
                exchange_name,
                exchange_config.get("market", "spot"),
            )
            continue

        try:
            collectors.extend(
                build_collectors(
                    exchange_name=exchange_name,
                    exchange_config=exchange_config,
                    output_handler=output_handler,
                    reconnect_base_seconds=reconnect_base_seconds,
                    reconnect_max_seconds=reconnect_max_seconds,
                )
            )
        except Exception as exc:
            logging.getLogger("main").exception(
                "Failed to initialize exchange=%s error=%s",
                exchange_name,
                exc,
            )

    if not collectors:
        raise ValueError("No valid collectors configured")
    return collectors


def _load_connection_entries(config: Dict) -> List[Tuple[str, Dict]]:
    logger = logging.getLogger("main")

    # Preferred format: explicit connection list
    connections = config.get("connections")
    if connections is not None:
        if not isinstance(connections, list):
            raise ValueError("'connections' must be a YAML list")

        entries: List[Tuple[str, Dict]] = []
        for idx, connection in enumerate(connections):
            if not isinstance(connection, dict):
                logger.error("Skipping non-object connection at index=%s", idx)
                continue
            exchange_name = str(connection.get("exchange", "")).strip().lower()
            if not exchange_name:
                logger.error("Skipping connection index=%s because 'exchange' is missing", idx)
                continue
            entries.append((exchange_name, connection))
        return entries

    # Backward-compatible fallback
    exchanges = config.get("exchanges", {})
    if not isinstance(exchanges, dict):
        return []
    entries: List[Tuple[str, Dict]] = []
    for exchange_name, exchange_config in exchanges.items():
        normalized_exchange = str(exchange_name).strip().lower()
        if not isinstance(exchange_config, dict):
            logger.error("Skipping non-object exchange config exchange=%s", normalized_exchange)
            continue

        markets = exchange_config.get("markets")
        if isinstance(markets, dict):
            # New nested format:
            # exchanges:
            #   binance:
            #     enabled: true
            #     markets:
            #       spot: {...}
            #       futures: {...}
            exchange_level = {
                key: value
                for key, value in exchange_config.items()
                if key != "markets"
            }
            exchange_enabled = bool(exchange_level.get("enabled", True))
            for market_name, market_config in markets.items():
                if not isinstance(market_config, dict):
                    logger.error(
                        "Skipping non-object market config exchange=%s market=%s",
                        normalized_exchange,
                        market_name,
                    )
                    continue

                merged_config = deepcopy(exchange_level)
                merged_config.update(market_config)
                merged_config["exchange"] = normalized_exchange
                merged_config["market"] = str(
                    merged_config.get("market", market_name)
                ).strip().lower()

                if "enabled" not in market_config:
                    merged_config["enabled"] = exchange_enabled

                merged_config.setdefault(
                    "id", f"{normalized_exchange}_{merged_config['market']}"
                )
                entries.append((normalized_exchange, merged_config))
            continue

        # Legacy object-per-exchange format
        entries.append((normalized_exchange, exchange_config))
    return entries


def load_topic_specs(topics_file: Path, kafka_settings: KafkaSettings) -> List[TopicSpec]:
    if not topics_file.exists():
        raise FileNotFoundError(f"Topics file not found: {topics_file}")

    topic_specs: List[TopicSpec] = []
    with topics_file.open("r", encoding="utf-8") as file_handle:
        for line_number, raw_line in enumerate(file_handle, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) not in {1, 3}:
                raise ValueError(
                    f"Invalid topics file format at {topics_file}:{line_number}. "
                    "Use '<topic>' or '<topic> <partitions> <replication_factor>'."
                )

            topic_name = parts[0]
            if len(parts) == 1:
                partitions = kafka_settings.topic_management.num_partitions
                replication_factor = kafka_settings.topic_management.replication_factor
            else:
                partitions = int(parts[1])
                replication_factor = int(parts[2])

            topic_specs.append(
                TopicSpec(
                    name=topic_name,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                )
            )

    if not topic_specs:
        raise ValueError(f"No topic entries found in {topics_file}")
    return topic_specs


async def run_pipeline(config: Dict) -> None:
    logger = logging.getLogger("main")
    kafka_settings = KafkaSettings.from_dict(config.get("kafka", {}))
    pipeline_config = config.get("pipeline", {})

    kafka_producer = KafkaDepthProducer(kafka_settings)
    try:
        topics_file = Path(kafka_settings.topic_management.topics_file)
        topic_specs = load_topic_specs(topics_file, kafka_settings)
        await kafka_producer.ensure_topics(topic_specs)
        await kafka_producer.start()

        data_processor = DataProcessor(max_levels=int(pipeline_config.get("max_levels", 50)))
        scd_processor = SCDProcessor()
        output_handler = OutputHandler(
            data_processor=data_processor,
            scd_processor=scd_processor,
            kafka_producer=kafka_producer,
            enable_scd_topic=bool(pipeline_config.get("enable_scd_topic", True)),
            stream_queue_maxsize=int(pipeline_config.get("stream_queue_maxsize", 1000)),
            queue_depth_log_interval_seconds=float(
                pipeline_config.get("queue_depth_log_interval_seconds", 10.0)
            ),
            queue_depth_warn_threshold=int(pipeline_config.get("queue_depth_warn_threshold", 800)),
        )

        collectors = create_collectors(config=config, output_handler=output_handler)
        collector_tasks = [
            asyncio.create_task(collector.run(), name=f"{collector.exchange}:{collector.symbol}")
            for collector in collectors
        ]

        logger.info(
            "Pipeline started collectors=%s normalized_topic=%s scd_topic=%s",
            len(collectors),
            kafka_settings.topics.normalized,
            kafka_settings.topics.scd,
        )
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        signal_registered = False
        for current_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(current_signal, stop_event.set)
                signal_registered = True
            except NotImplementedError:
                pass

        stop_task = asyncio.create_task(stop_event.wait()) if signal_registered else None
        wait_targets = list(collector_tasks)
        if stop_task is not None:
            wait_targets.append(stop_task)

        done, _ = await asyncio.wait(wait_targets, return_when=asyncio.FIRST_COMPLETED)
        for completed in done:
            if completed is stop_task:
                logger.info("Shutdown signal received")
            elif completed.exception() is not None:
                logger.error("Collector task failed error=%s", completed.exception())
            else:
                logger.warning("Collector task exited unexpectedly task=%s", completed.get_name())
    finally:
        collectors = locals().get("collectors", [])
        collector_tasks = locals().get("collector_tasks", [])
        stop_task = locals().get("stop_task")
        for collector in collectors:
            collector.stop()
        for task in collector_tasks:
            task.cancel()
        if collector_tasks:
            await asyncio.gather(*collector_tasks, return_exceptions=True)
        if stop_task is not None:
            stop_task.cancel()
        output_handler = locals().get("output_handler")
        if output_handler is not None:
            await output_handler.stop()
        await kafka_producer.stop()
        logger.info("Pipeline stopped cleanly")


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config))
    configure_logging(config.get("pipeline", {}).get("log_level", "INFO"))
    asyncio.run(run_pipeline(config))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
