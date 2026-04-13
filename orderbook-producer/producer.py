# producer.py
import os
import json
import time
import hashlib
import threading
import traceback
from collections import deque

import requests
import websocket
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
EXCHANGE = os.getenv("EXCHANGE", "binance")
MARKET = os.getenv("MARKET", "spot")

TOPIC_ORDERBOOK = os.getenv(
    "TOPIC_ORDERBOOK",
    os.getenv("KAFKA_TOPIC", "orderbook.btcusdt")
)

REST_SNAPSHOT_URL = os.getenv("REST_SNAPSHOT_URL", "https://api.binance.com/api/v3/depth")
WS_URL = os.getenv("WS_URL", "wss://stream.binance.com/ws")
SNAPSHOT_LIMIT = int(os.getenv("SNAPSHOT_LIMIT", "1000"))
HEARTBEAT_INTERVAL_MS = int(os.getenv("HEARTBEAT_INTERVAL_MS", "5000"))
WS_STALE_RECONNECT_MS = int(os.getenv("WS_STALE_RECONNECT_MS", "15000"))

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
})

def now_ms() -> int:
    return int(time.time() * 1000)

def make_event_id(exchange: str, market: str, symbol: str, event_type: str, u_from, u_to, snapshot_id=None) -> str:
    base = f"{exchange}|{market}|{symbol}|{event_type}|{u_from}|{u_to}|{snapshot_id or ''}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def send_event(topic: str, key: str, value: dict) -> None:
    producer.produce(topic=topic, key=key, value=json.dumps(value).encode("utf-8"))
    producer.poll(0)

def normalize_snapshot(symbol: str, data: dict) -> dict:
    snapshot_id = int(data["lastUpdateId"])
    bids = data.get("bids", [])
    asks = data.get("asks", [])
    evt = {
        "exchange": EXCHANGE,
        "market": MARKET,
        "symbol": symbol,
        "type": "snapshot",
        "event_time_ms": now_ms(),
        "snapshot_last_update_id": snapshot_id,
        "update_id_from": snapshot_id,
        "update_id_to": snapshot_id,
        "bids": bids,
        "asks": asks,
    }
    evt["event_id"] = make_event_id(EXCHANGE, MARKET, symbol, "snapshot", snapshot_id, snapshot_id, snapshot_id)
    return evt

def normalize_delta(symbol: str, msg: dict) -> dict:
    u_from = int(msg["U"])
    u_to = int(msg["u"])
    bids = msg.get("b", [])
    asks = msg.get("a", [])
    evt = {
        "exchange": EXCHANGE,
        "market": MARKET,
        "symbol": symbol,
        "type": "delta",
        "event_time_ms": int(msg.get("E", now_ms())),
        "update_id_from": u_from,
        "update_id_to": u_to,
        "bids": bids,
        "asks": asks,
    }
    evt["event_id"] = make_event_id(EXCHANGE, MARKET, symbol, "delta", u_from, u_to)
    return evt

def fetch_snapshot(symbol: str, limit: int = 1000) -> dict:
    r = requests.get(REST_SNAPSHOT_URL, params={"symbol": symbol, "limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json()

def main():
    key = f"{EXCHANGE}|{MARKET}|{SYMBOL}"
    stream = f"{SYMBOL.lower()}@depth@100ms"  # IMPORTANT: 100ms diff depth stream

    buffer = deque(maxlen=20000)
    buffer_lock = threading.Lock()

    ws_ready = threading.Event()
    aligned = threading.Event()
    resync_requested = threading.Event()

    # Sequencing state (producer-side gap detection)
    expected_next_u = None
    expected_lock = threading.Lock()
    last_ws_u = None
    last_ws_msg_ms = 0
    last_resync_buffer_log_ms = 0
    last_heartbeat_ms = 0
    resync_epoch = 0
    last_stale_reconnect_ms = 0
    ws_lock = threading.Lock()
    ws_holder = {"ws": None}

    def snapshot_buffer():
        with buffer_lock:
            return list(buffer)

    def buffer_range_from_list(buf_list):
        if not buf_list:
            return "empty"
        first = int(buf_list[0]["update_id_from"])
        last = int(buf_list[-1]["update_id_to"])
        return f"{first}..{last}"

    def request_resync(reason: str, keep_evt=None, reset_expected: bool = True):
        """Request a resync (snapshot + buffer realign)."""
        nonlocal expected_next_u, resync_epoch
        resync_epoch += 1
        before = snapshot_buffer()
        resync_requested.set()
        aligned.clear()
        if reset_expected:
            with expected_lock:
                expected_next_u = None
        with buffer_lock:
            buffer.clear()
            if keep_evt is not None:
                buffer.append(keep_evt)
            after_size = len(buffer)
        print(
            f"[producer] RESYNC requested epoch={resync_epoch}: {reason}. "
            f"buffer_before_size={len(before)} range={buffer_range_from_list(before)} "
            f"buffer_after_size={after_size}"
        )

    def maybe_log_heartbeat(context: str = "loop"):
        nonlocal last_heartbeat_ms
        now = now_ms()
        if now - last_heartbeat_ms < HEARTBEAT_INTERVAL_MS:
            return
        last_heartbeat_ms = now
        with buffer_lock:
            bsz = len(buffer)
        with expected_lock:
            exp = expected_next_u
        ws_age_ms = (now - last_ws_msg_ms) if last_ws_msg_ms else -1
        print(
            "[producer] heartbeat "
            f"context={context} aligned={aligned.is_set()} resync={resync_requested.is_set()} "
            f"buffer_size={bsz} last_ws_u={last_ws_u} expected_next_u={exp} ws_age_ms={ws_age_ms}"
        )

    def ws_watchdog_loop():
        nonlocal last_stale_reconnect_ms
        while True:
            time.sleep(1.0)
            if not ws_ready.is_set():
                continue
            if last_ws_msg_ms <= 0:
                continue
            age = now_ms() - last_ws_msg_ms
            if age < WS_STALE_RECONNECT_MS:
                continue
            # Debounce forced reconnect attempts.
            now = now_ms()
            if now - last_stale_reconnect_ms < WS_STALE_RECONNECT_MS:
                continue
            last_stale_reconnect_ms = now
            print(
                f"[producer] ws stale detected age_ms={age} >= {WS_STALE_RECONNECT_MS}. "
                "Forcing reconnect..."
            )
            with ws_lock:
                ws_obj = ws_holder["ws"]
            if ws_obj is not None:
                try:
                    ws_obj.close()
                except Exception as e:
                    print(f"[producer] ws close during stale-reconnect failed: {e}")
    
    def wait_for_buffer(min_items: int, timeout_s: float = 3.0) -> int:
        """Wait until buffer has at least min_items items (or timeout). Returns current size."""
        t0 = time.time()
        while True:
            with buffer_lock:
                n = len(buffer)
            if n >= min_items:
                return n
            if time.time() - t0 >= timeout_s:
                return n
            time.sleep(0.05)

    def find_bridging_delta_in_buffer(buf_list, target: int):
        for i, evt in enumerate(buf_list):
            U = int(evt["update_id_from"])
            u = int(evt["update_id_to"])
            if U <= target <= u:
                return i
        return None

    def on_open(ws):
        ws_ready.set()
        print("[producer] ws connected, buffering deltas...")

    def on_message(ws, message):
        nonlocal expected_next_u, last_ws_u, last_ws_msg_ms, last_resync_buffer_log_ms
        try:
            msg = json.loads(message)
            delta_evt = normalize_delta(SYMBOL, msg)
            last_ws_u = int(delta_evt["update_id_to"])
            last_ws_msg_ms = now_ms()

            # While not aligned (or resyncing), just buffer.
            if not aligned.is_set():
                with buffer_lock:
                    buffer.append(delta_evt)
                    size = len(buffer)
                now = now_ms()
                if resync_requested.is_set() and now - last_resync_buffer_log_ms >= 2000:
                    last_resync_buffer_log_ms = now
                    buf_list = snapshot_buffer()
                    print(
                        "[producer] resync buffering progress "
                        f"size={size} range={buffer_range_from_list(buf_list)} last_ws_u={last_ws_u}"
                    )
                return

            u_from = int(delta_evt["update_id_from"])
            u_to = int(delta_evt["update_id_to"])

            # Producer-side gap detection after alignment.
            gap_reason = None
            with expected_lock:
                exp = expected_next_u

                if exp is None:
                    # Should not happen often, but be safe.
                    expected_next_u = u_to + 1
                    send_event(TOPIC_ORDERBOOK, key, delta_evt)
                    return

                if u_to < exp:
                    # Fully stale / replay.
                    return

                if u_from > exp:
                    # REAL GAP: we missed updates on WS -> must resync.
                    gap_reason = f"GAP detected: expected U={exp} but got U={u_from}..u={u_to}"
                else:
                    # Overlap or exact continuation is OK: u_from <= exp <= u_to
                    expected_next_u = u_to + 1

            if gap_reason is not None:
                # IMPORTANT: do not call request_resync while holding expected_lock.
                request_resync(gap_reason, keep_evt=delta_evt, reset_expected=False)
                return

            send_event(TOPIC_ORDERBOOK, key, delta_evt)
        except Exception as e:
            print(f"[producer] ERROR in on_message: {e}")
            print(traceback.format_exc())

    def on_error(ws, error):
        print(f"[producer] ws error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print(f"[producer] ws closed code={close_status_code} msg={close_msg}")

    def ws_loop():
        while True:
            try:
                ws = websocket.WebSocketApp(
                    f"{WS_URL}/{stream}",
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                with ws_lock:
                    ws_holder["ws"] = ws
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                print(f"[producer] ws loop exception: {e}")
                print(traceback.format_exc())
            finally:
                with ws_lock:
                    ws_holder["ws"] = None
            print("[producer] ws loop exited; reconnecting in 1s...")
            time.sleep(1)

    threading.Thread(target=ws_loop, daemon=True).start()
    threading.Thread(target=ws_watchdog_loop, daemon=True).start()

    wait_rounds = 0
    while not ws_ready.wait(timeout=2):
        wait_rounds += 1
        if wait_rounds % 5 == 0:
            print("[producer] waiting for WebSocket connection...")
        maybe_log_heartbeat("wait_ws")

    # Wait until we have some deltas buffered
    t0 = time.time()
    while True:
        with buffer_lock:
            n = len(buffer)
        if n >= 50:
            break
        if time.time() - t0 > 3:
            break
        time.sleep(0.05)
    print(f"[producer] buffered {n} deltas before snapshot")

    # Snapshot + buffer alignment loop (also used for resync)
    while True:
        resync_requested.clear()

        # Retry snapshot+align until success
        while not aligned.is_set():
            # Warm up buffer before taking snapshot (VERY IMPORTANT after resync)
            n = wait_for_buffer(min_items=200, timeout_s=5.0)
            warm_buf = snapshot_buffer()
            print(
                "[producer] buffer warmup "
                f"size={n} range={buffer_range_from_list(warm_buf)} last_ws_u={last_ws_u} before snapshot"
            )

            snap_raw = fetch_snapshot(SYMBOL, SNAPSHOT_LIMIT)
            snap_evt = normalize_snapshot(SYMBOL, snap_raw)
            last_update_id = int(snap_evt["snapshot_last_update_id"])
            target = last_update_id + 1

            send_event(TOPIC_ORDERBOOK, key, snap_evt)
            print(f"[producer] sent snapshot topic={TOPIC_ORDERBOOK} lastUpdateId={last_update_id} target={target}")

            # Deterministic align: keep the same snapshot target for a short window.
            # If target is ahead of current buffer tail, wait for more deltas instead of
            # immediately moving the target via a new snapshot.
            align_deadline = time.time() + 4.0
            start_idx = None
            buf_list = []
            while time.time() < align_deadline:
                buf_list = snapshot_buffer()
                start_idx = find_bridging_delta_in_buffer(buf_list, target)
                if start_idx is not None:
                    break
                time.sleep(0.05)

            if start_idx is None:
                if buf_list:
                    first = buf_list[0]
                    last = buf_list[-1]
                    print(
                        "[producer] No bridging delta yet for "
                        f"target={target}; buffer range={first['update_id_from']}..{last['update_id_to']}. "
                        "Retrying with a fresh snapshot..."
                    )
                else:
                    print(f"[producer] No bridging delta yet for target={target}; buffer empty. Retrying snapshot...")
                continue

            # Publish buffered deltas from bridging point and compute expected_next_u
            last_u_published = None
            for evt in buf_list[start_idx:]:
                send_event(TOPIC_ORDERBOOK, key, evt)
                last_u_published = int(evt["update_id_to"])

            with expected_lock:
                expected_next_u = (last_u_published + 1) if last_u_published is not None else (target)

            aligned.set()
            print(
                f"[producer] aligned using buffer idx={start_idx}. "
                f"streaming live deltas on {TOPIC_ORDERBOOK} expected_next_u={expected_next_u} "
                f"buffer_size={len(buf_list)} bridge_delta={buf_list[start_idx]['update_id_from']}..{buf_list[start_idx]['update_id_to']}"
            )

        # Stay alive; if on_message requests resync, this loop will realign.
        while aligned.is_set() and not resync_requested.is_set():
            time.sleep(0.2)
            maybe_log_heartbeat("aligned")

        if resync_requested.is_set():
            # Loop back to snapshot+align
            continue

if __name__ == "__main__":
    main()
