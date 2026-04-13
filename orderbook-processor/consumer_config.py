import os
import re
from dataclasses import dataclass

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None


@dataclass
class ConsumerRuntimeConfig:
    group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool


def resolve_consumer_runtime_config(default_config_path: str = "config/config.yaml") -> ConsumerRuntimeConfig:
    config_path = os.getenv("CONSUMER_CONFIG_PATH", default_config_path)
    config = _load_yaml_config(config_path)

    group_id = _resolve_group_id(config=config)
    auto_offset_reset = str(
        os.getenv("AUTO_OFFSET_RESET", _deep_get(config, "consumer.kafka.auto_offset_reset", "earliest"))
    )
    enable_auto_commit = _resolve_bool(
        os.getenv("ENABLE_AUTO_COMMIT", _deep_get(config, "consumer.kafka.enable_auto_commit", True))
    )

    return ConsumerRuntimeConfig(
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
    )


def _resolve_group_id(config: dict) -> str:
    explicit_group_id = os.getenv("GROUP_ID")
    if explicit_group_id:
        return explicit_group_id

    env_name = os.getenv("CONSUMER_ENV", _deep_get(config, "consumer.group.env", os.getenv("APP_ENV", "dev")))
    role = os.getenv("CONSUMER_ROLE", _deep_get(config, "consumer.group.role", "sink.clickhouse"))
    layer = os.getenv("CONSUMER_LAYER", _deep_get(config, "consumer.group.layer", "scd"))
    version_raw = os.getenv("CONSUMER_GROUP_VERSION", _deep_get(config, "consumer.group.version", 1))
    try:
        version = int(version_raw)
    except (TypeError, ValueError):
        version = 1

    env_part = _sanitize_component(env_name, "dev")
    role_part = _sanitize_component(role, "sink.clickhouse")
    layer_part = _sanitize_component(layer, "scd")
    return f"md.{env_part}.{role_part}.{layer_part}.v{version}"


def _load_yaml_config(config_path: str) -> dict:
    if yaml is None:
        return {}
    if not config_path:
        return {}
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r", encoding="utf-8") as config_file:
        loaded = yaml.safe_load(config_file) or {}
    return loaded if isinstance(loaded, dict) else {}


def _deep_get(data: dict, path: str, default=None):
    node = data
    for part in path.split("."):
        if not isinstance(node, dict):
            return default
        if part not in node:
            return default
        node = node[part]
    return node


def _sanitize_component(value: str, fallback: str) -> str:
    cleaned = str(value or "").strip().lower()
    if not cleaned:
        cleaned = fallback
    cleaned = re.sub(r"[^a-z0-9._-]+", "-", cleaned)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or fallback


def _resolve_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    return normalized not in {"0", "false", "no", "off"}
