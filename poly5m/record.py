
from __future__ import annotations

import json
import os
import queue
import threading
import time
from datetime import datetime, timezone

import websocket

from poly5m.discover import fetch_market, window_start


CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"

DATA_DIR = "data"
RECONNECT_DELAY = 3
DISCOVERY_INTERVAL = 3
HEARTBEAT_INTERVAL = 10

records = queue.Queue(maxsize=100000)
stop_event = threading.Event()

state_lock = threading.Lock()
send_lock = threading.Lock()
stats_lock = threading.Lock()

state = {
    "clob": None,
    "market": None,
    "tokens": set(),
}

stats = {
    "clob_events": 0,
    "btc_ticks": 0,
    "records_written": 0,
    "records_dropped": 0,
}

last_btc_log = 0.0
last_writer_log = time.time()


def log(message):
    print(message, flush=True)


def save_record(record):
    with stats_lock:
        try:
            records.put_nowait(record)
        except queue.Full:
            stats["records_dropped"] += 1
            dropped = stats["records_dropped"]

            if dropped == 1 or dropped % 100 == 0:
                log(f"[WRITER WARNING] Records dropped: {dropped}")


def writer_loop():
    global last_writer_log

    os.makedirs(DATA_DIR, exist_ok=True)
    current_day = None
    handle = None
    pending_flush = 0

    try:
        while not stop_event.is_set() or not records.empty():
            try:
                record = records.get(timeout=1)
            except queue.Empty:
                if handle:
                    handle.flush()

                continue

            try:
                rx = float(record.get("rx", time.time()))
                day = datetime.fromtimestamp(
                    rx, timezone.utc
                ).strftime("%Y%m%d")

                if day != current_day:
                    if handle:
                        handle.flush()
                        handle.close()

                    path = os.path.join(
                        DATA_DIR, f"poly5m_{day}.jsonl"
                    )
                    handle = open(path, "a", encoding="utf-8")
                    current_day = day
                    log(f"[WRITER] File open: {path}")

                handle.write(
                    json.dumps(
                        record,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ) + "\n"
                )

                pending_flush += 1

                if pending_flush >= 20:
                    handle.flush()
                    pending_flush = 0

                with stats_lock:
                    stats["records_written"] += 1
                    written = stats["records_written"]

                now = time.time()

                if written == 1 or now - last_writer_log >= 30:
                    if handle:
                        handle.flush()

                    with stats_lock:
                        snapshot = dict(stats)

                    log(
                        f"[WRITER] written={snapshot['records_written']} "
                        f"queue={records.qsize()} "
                        f"dropped={snapshot['records_dropped']}"
                    )
                    last_writer_log = now

            except Exception as exc:
                log(f"[WRITER ERROR] {exc}")

            finally:
                records.task_done()

    finally:
        if handle:
            handle.flush()
            handle.close()


def send_clob(payload):
    with send_lock:
        ws = state["clob"]

        if ws is None or ws.sock is None or not ws.sock.connected:
            return False

        try:
            ws.send(json.dumps(payload))
            return True
        except Exception as exc:
            log(f"[CLOB SEND ERROR] {exc}")
            return False


def set_market(market):
    new_tokens = {
        str(market["up"]),
        str(market["down"]),
    }

    with state_lock:
        old_market = state["market"]
        old_tokens = set(state["tokens"])

        if old_market and old_market.get("start") == market.get("start"):
            return

        state["market"] = market
        state["tokens"] = new_tokens

        connected = state["clob"] is not None

    save_record({
        "rx": time.time(),
        "meta": market,
    })

    log(
        f"[MARKET] {market.get('slug')} "
        f"UP={market['up']} DOWN={market['down']}"
    )

    if not connected:
        return

    if old_tokens:
        send_clob({
            "assets_ids": list(old_tokens),
            "operation": "unsubscribe",
            "custom_feature_enabled": True,
        })

    if send_clob({
        "assets_ids": list(new_tokens),
        "operation": "subscribe",
        "custom_feature_enabled": True,
    }):
        log("[CLOB] Current market subscription requested")


def discover_loop():
    while not stop_event.is_set():
        try:
            start = window_start(time.time())

            with state_lock:
                known = state["market"]
                already_known = (
                    known is not None and known.get("start") == start
                )

            if not already_known:
                market = fetch_market(start)

                if market:
                    set_market(market)

        except Exception as exc:
            log(f"[DISCOVERY ERROR] {exc}")

        stop_event.wait(DISCOVERY_INTERVAL)


def clob_on_open(ws):
    with state_lock:
        state["clob"] = ws
        market = state["market"]

    log("[CLOB] Connected")

    if market:
        payload = {
            "assets_ids": [
                str(market["up"]),
                str(market["down"]),
            ],
            "type": "market",
            "custom_feature_enabled": True,
        }

        if send_clob(payload):
            log("[CLOB] Current market subscribed")


def clob_heartbeat(ws):
    while not stop_event.wait(HEARTBEAT_INTERVAL):
        with state_lock:
            if state["clob"] is not ws:
                return

        try:
            with send_lock:
                if ws.sock is None or not ws.sock.connected:
                    return

                ws.send("PING")

        except Exception as exc:
            log(f"[CLOB HEARTBEAT ERROR] {exc}")
            return


def describe_clob_event(event):
    if not isinstance(event, dict):
        return str(type(event).__name__)

    event_type = event.get("event_type", event.get("type", "unknown"))
    asset = event.get("asset_id", "")

    details = []

    if "best_bid" in event:
        details.append(f"best_bid={event['best_bid']}")

    if "best_ask" in event:
        details.append(f"best_ask={event['best_ask']}")

    if event_type == "price_change":
        changes = event.get("price_changes", [])

        for change in changes[:2]:
            if not isinstance(change, dict):
                continue

            token = change.get("asset_id", "")
            bid = change.get("best_bid")
            ask = change.get("best_ask")

            details.append(
                f"token={token} bid={bid} ask={ask}"
            )

    if event_type == "book":
        bids = event.get("bids") or []
        asks = event.get("asks") or []

        if bids:
            details.append(f"bid={bids[0].get('price')}")

        if asks:
            details.append(f"ask={asks[0].get('price')}")

    suffix = " ".join(str(item) for item in details)

    return f"type={event_type} asset={asset} {suffix}".strip()


def clob_on_message(ws, message):
    if not message:
        return

    if message == "PING":
        try:
            ws.send("PONG")
        except Exception as exc:
            log(f"[CLOB PONG ERROR] {exc}")

        return

    if message == "PONG":
        return

    try:
        event = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        log("[CLOB WARNING] Non-JSON message ignored")
        return

    save_record({
        "rx": time.time(),
        "ev": event,
    })

    events = event if isinstance(event, list) else [event]

    for item in events:
        with stats_lock:
            stats["clob_events"] += 1
            count = stats["clob_events"]

        # Limita i log: primi 5 eventi, poi uno ogni 50.
        if count <= 5 or count % 50 == 0:
            log(
                f"[CLOB DATA] event={count} "
                f"{describe_clob_event(item)}"
            )


def clob_on_error(ws, error):
    log(f"[CLOB ERROR] {error}")


def clob_on_close(ws, code, message):
    with state_lock:
        if state["clob"] is ws:
            state["clob"] = None

    log(f"[CLOB CLOSED] {code} {message}")


def clob_loop():
    while not stop_event.is_set():
        ws = websocket.WebSocketApp(
            CLOB_URL,
            on_open=clob_on_open,
            on_message=clob_on_message,
            on_error=clob_on_error,
            on_close=clob_on_close,
        )

        try:
            ws.run_forever()
        except Exception as exc:
            log(f"[CLOB LOOP ERROR] {exc}")

        with state_lock:
            if state["clob"] is ws:
                state["clob"] = None

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)


def coinbase_on_open(ws):
    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker"],
    }))

    log("[BTC] WebSocket CONNECTED")
    log("[BTC] Coinbase ticker subscribed")


def coinbase_on_message(ws, message):
    global last_btc_log

    try:
        event = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        return

    if not isinstance(event, dict) or event.get("type") != "ticker":
        return

    try:
        price = float(event["price"])
        timestamp = event.get("time")

        timestamp_ms = (
            int(datetime.fromisoformat(
                timestamp.replace("Z", "+00:00")
            ).timestamp() * 1000)
            if timestamp
            else int(time.time() * 1000)
        )
    except (KeyError, TypeError, ValueError):
        return

    save_record({
        "rx": time.time(),
        "ref": {
            "T": timestamp_ms,
            "p": price,
            "source": "coinbase",
        },
    })

    with stats_lock:
        stats["btc_ticks"] += 1
        count = stats["btc_ticks"]

    now = time.time()

    if count == 1 or now - last_btc_log >= 10:
        log(f"[BTC DATA] BTC-USD={price:.2f} ticks={count}")
        last_btc_log = now


def coinbase_on_error(ws, error):
    log(f"[BTC ERROR] {error}")


def coinbase_on_close(ws, code, message):
    log(f"[BTC CLOSED] {code} {message}")


def coinbase_loop():
    while not stop_event.is_set():
        ws = websocket.WebSocketApp(
            COINBASE_URL,
            on_open=coinbase_on_open,
            on_message=coinbase_on_message,
            on_error=coinbase_on_error,
            on_close=coinbase_on_close,
        )

        try:
            ws.run_forever()
        except Exception as exc:
            log(f"[BTC LOOP ERROR] {exc}")

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)


def main():
    log("=" * 60)
    log("POLYMARKET BTC UP/DOWN 5M - PAPER RECORDER")
    log("CURRENT MARKET ONLY | DIAGNOSTICS ENABLED")
    log("NO ORDERS - DATA COLLECTION ONLY")
    log("=" * 60)

    threading.Thread(
        target=writer_loop,
        name="jsonl-writer",
        daemon=True,
    ).start()

    threading.Thread(
        target=discover_loop,
        name="market-discovery",
        daemon=True,
    ).start()

    threading.Thread(
        target=clob_loop,
        name="clob-recorder",
        daemon=True,
    ).start()

    coinbase_loop()


if __name__ == "__main__":
    main()
