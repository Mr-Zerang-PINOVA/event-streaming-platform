from typing import Dict, List

from .binance_collector import BinanceCollector
from .bybit_collector import BybitCollector
from .kucoin_collector import KucoinCollector
from .okx_collector import OKXCollector


COLLECTOR_REGISTRY = {
    "binance": BinanceCollector,
    "bybit": BybitCollector,
    "kucoin": KucoinCollector,
    "okx": OKXCollector,
}

DEFAULT_WS_URLS = {
    "binance": {
        "spot": "wss://stream.binance.com/ws",
        "futures": "wss://fstream.binance.com/ws",
        "perp": "wss://fstream.binance.com/ws",
    },
    "bybit": {
        "spot": "wss://stream.bybit.com/v5/public/spot",
        "linear": "wss://stream.bybit.com/v5/public/linear",
    },
    "okx": {
        "spot": "wss://ws.okx.com:8443/ws/v5/public",
        "margin": "wss://ws.okx.com:8443/ws/v5/public",
        "swap": "wss://ws.okx.com:8443/ws/v5/public",
    },
    "kucoin": {
        "spot": "wss://ws-api-spot.kucoin.com/",
    },
}


def build_collectors(
    exchange_name: str,
    exchange_config: Dict,
    output_handler,
    reconnect_base_seconds: float,
    reconnect_max_seconds: float,
) -> List:
    normalized_exchange = exchange_name.lower()
    collector_class = COLLECTOR_REGISTRY.get(normalized_exchange)
    if collector_class is None:
        raise ValueError(f"Unsupported exchange '{exchange_name}'")

    market = str(exchange_config.get("market", "spot")).lower()
    ws_url = exchange_config.get("ws_url") or DEFAULT_WS_URLS.get(normalized_exchange, {}).get(market)
    if not ws_url:
        raise ValueError(
            f"Missing ws_url for exchange='{exchange_name}' market='{market}'. "
            "Set it explicitly in config."
        )

    symbols = exchange_config.get("symbols")
    if symbols is None:
        single_symbol = exchange_config.get("symbol")
        symbols = [single_symbol] if single_symbol else []

    if not isinstance(symbols, list) or not symbols:
        raise ValueError(f"Missing symbols for exchange '{exchange_name}'")

    collector_options = {
        key: value
        for key, value in exchange_config.items()
        if key not in {"id", "exchange", "enabled", "ws_url", "symbol", "symbols", "market"}
    }

    collectors = []
    for symbol in symbols:
        collectors.append(
            collector_class(
                exchange=normalized_exchange,
                ws_url=ws_url,
                symbol=str(symbol),
                market=market,
                output_handler=output_handler,
                reconnect_base_seconds=reconnect_base_seconds,
                reconnect_max_seconds=reconnect_max_seconds,
                **collector_options,
            )
        )
    return collectors
