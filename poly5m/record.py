
from __future__ import annotations

import json
import os
import queue
import threading
import time
from datetime import datetime, timezone

import websocket

from poly5m.discover import fetch_market, window_start, WINDOW


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

state = {
    "clob": None,
    "market": None,
    "tokens": set(),
}


def log(message):
    print(message, flush=True)


def save_record(record):
    try:
        records.put_nowait(record)
    except queue.Full:
        log("[WRITER WARNING] Queue full; record dropped")


def writer_loop():
    os.makedirs(DATA_DIR, exist_ok=True)
    current_day = None
    handle = None

    try:
        while not stop_event.is_set() or not records.empty():
            try:
                record = records.get(timeout=1)
            except queue.Empty:
                continue

            try:
                day = datetime.fromtimestamp(
                    record.get("rx", time.time()),
                    timezone.utc,
                ).strftime("%Y%m%d")

                if day != current_day:
                    if handle:
                        handle.close()

                    handle = open(
                        os.path.join(DATA_DIR, f"poly5m_{day}.jsonl"),
                        "a",
                        encoding="utf-8",
                    )
                    current_day = day

                handle.write(
                    json.dumps(record, separators=(",", ":")) + "\n"
                )
            except Exception as exc:
                log(f"[WRITER ERROR] {exc}")
            finally:
                records.task_done()
    finally:
        if handle:
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

    save_record({
        "rx": time.time(),
        "meta": market,
    })

    log(
        f"[MARKET] {market.get('slug')} "
        f"UP={market['up']} DOWN={market['down']}"
    )

    with state_lock:
        connected = state["clob"] is not None

    if not connected:
        return

    if old_tokens:
        send_clob({
            "assets_ids": list(old_tokens),
            "operation": "unsubscribe",
            "custom_feature_enabled": True,
        })

    send_clob({
        "assets_ids": list(new_tokens),
        "operation": "subscribe",
        "custom_feature_enabled": True,
    })

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
        send_clob({
            "assets_ids": [str(market["up"]), str(market["down"])],
            "type": "market",
            "custom_feature_enabled": True,
        })
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


def clob_on_message(ws, message):
    if message == "PING":
        try:
            ws.send("PONG")
        except Exception:
            pass
        return

    if message == "PONG" or not message:
        return

    try:
        event = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        return

    save_record({
        "rx": time.time(),
        "ev": event,
    })


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

        threading.Thread(
            target=clob_heartbeat,
            args=(ws,),
            daemon=True,
        ).start()

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
            if timestamp else int(time.time() * 1000)
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
    log("CURRENT MARKET ONLY | COINBASE TICKER")
    log("NO ORDERS - DATA COLLECTION ONLY")
    log("=" * 60)

    threading.Thread(target=writer_loop, daemon=True).start()
    threading.Thread(target=discover_loop, daemon=True).start()
    threading.Thread(target=clob_loop, daemon=True).start()

    coinbase_loop()


if __name__ == "__main__":
    main()
