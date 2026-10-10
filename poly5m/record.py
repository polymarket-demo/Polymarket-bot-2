
from __future__ import annotations

import json
import os
import queue
import threading
import time
from datetime import datetime, timezone

import websocket

from poly5m.discover import fetch_market, window_start, WINDOW


WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"

DATA_DIR = "data"
WRITE_QUEUE_SIZE = 100000
DISCOVERY_INTERVAL = 3
RECONNECT_DELAY = 3
HEARTBEAT_INTERVAL = 10

CUSTOM_FEATURES = True

write_queue = queue.Queue(maxsize=WRITE_QUEUE_SIZE)
stop_event = threading.Event()

market_lock = threading.Lock()
ws_lock = threading.Lock()

# Mercati attivi, indicizzati per inizio finestra.
active_markets = {}

# Token attualmente sottoscritti al CLOB.
subscribed = set()

market_ws = {"app": None}


def log(message):
    print(message, flush=True)


def enqueue(record):
    try:
        write_queue.put_nowait(record)
    except queue.Full:
        log("[WRITER WARNING] Coda piena: record scartato")


def writer_loop():
    os.makedirs(DATA_DIR, exist_ok=True)
    current_day = None
    handle = None

    try:
        while not stop_event.is_set() or not write_queue.empty():
            try:
                record = write_queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                rx = float(record.get("rx", time.time()))
                day = datetime.fromtimestamp(
                    rx, timezone.utc
                ).strftime("%Y%m%d")

                if day != current_day:
                    if handle:
                        handle.close()

                    path = os.path.join(
                        DATA_DIR, f"poly5m_{day}.jsonl"
                    )
                    handle = open(path, "a", encoding="utf-8")
                    current_day = day

                handle.write(
                    json.dumps(
                        record,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            except Exception as exc:
                log(f"[WRITER ERROR] {exc}")
            finally:
                write_queue.task_done()

    finally:
        if handle:
            handle.close()


def token_list(markets):
    result = []

    for market in markets:
        for key in ("up", "down"):
            token = market.get(key)
            if token:
                result.append(str(token))

    return list(dict.fromkeys(result))


def subscribe_tokens(tokens, initial=False):
    tokens = list(dict.fromkeys(str(t) for t in tokens if t))

    if not tokens:
        return

    with ws_lock:
        app = market_ws["app"]

        if app is None or app.sock is None or not app.sock.connected:
            return

        new_tokens = [t for t in tokens if t not in subscribed]

        if not new_tokens:
            return

        if initial:
            payload = {
                "assets_ids": new_tokens,
                "type": "market",
                "custom_feature_enabled": CUSTOM_FEATURES,
            }
        else:
            payload = {
                "assets_ids": new_tokens,
                "operation": "subscribe",
                "custom_feature_enabled": CUSTOM_FEATURES,
            }

        try:
            app.send(json.dumps(payload))
            subscribed.update(new_tokens)
            log(
                f"[CLOB] Subscription requested: "
                f"{len(new_tokens)} tokens"
            )
        except Exception as exc:
            log(f"[CLOB SUBSCRIBE ERROR] {exc}")


def unsubscribe_tokens(tokens):
    tokens = list(dict.fromkeys(str(t) for t in tokens if t))

    if not tokens:
        return

    with ws_lock:
        app = market_ws["app"]

        if app is None or app.sock is None or not app.sock.connected:
            return

        old_tokens = [t for t in tokens if t in subscribed]

        if not old_tokens:
            return

        payload = {
            "assets_ids": old_tokens,
            "operation": "unsubscribe",
            "custom_feature_enabled": CUSTOM_FEATURES,
        }

        try:
            app.send(json.dumps(payload))
            subscribed.difference_update(old_tokens)
            log(
                f"[CLOB] Unsubscribe requested: "
                f"{len(old_tokens)} old tokens"
            )
        except Exception as exc:
            log(f"[CLOB UNSUBSCRIBE ERROR] {exc}")


def discover_loop():
    while not stop_event.is_set():
        now = time.time()
        current_start = window_start(now)

        # Conserva il mercato precedente per intercettarne
        # eventuali aggiornamenti di risoluzione.
        with market_lock:
            expired_starts = [
                start
                for start in active_markets
                if start < current_start - WINDOW
            ]

            expired_tokens = []

            for start in expired_starts:
                market = active_markets.pop(start)
                expired_tokens.extend(token_list([market]))

        if expired_tokens:
            unsubscribe_tokens(expired_tokens)

        # Cerca il mercato corrente e quello successivo.
        for start in (current_start, current_start + WINDOW):
            with market_lock:
                already_known = start in active_markets

            if already_known:
                continue

            try:
                market = fetch_market(start)

                if not market:
                    continue

                with market_lock:
                    if start in active_markets:
                        continue
                    active_markets[start] = market

                # Metadati registrati una sola volta per mercato.
                enqueue({
                    "rx": time.time(),
                    "meta": market,
                })

                log(
                    f"[MARKET] {market.get('slug')} "
                    f"UP={market.get('up')} "
                    f"DOWN={market.get('down')}"
                )

                subscribe_tokens(token_list([market]))

            except Exception as exc:
                log(f"[DISCOVERY ERROR] {exc}")

        stop_event.wait(DISCOVERY_INTERVAL)


def on_market_open(ws):
    with ws_lock:
        market_ws["app"] = ws

    log("[CLOB] Connected")

    with market_lock:
        markets = list(active_markets.values())

    subscribe_tokens(token_list(markets), initial=True)

    # Heartbeat applicativo: messaggio testuale PING.
    threading.Thread(
        target=heartbeat_loop,
        args=(ws,),
        daemon=True,
    ).start()


def heartbeat_loop(ws):
    while not stop_event.wait(HEARTBEAT_INTERVAL):
        try:
            with ws_lock:
                if market_ws["app"] is not ws:
                    return

                if ws.sock is None or not ws.sock.connected:
                    return

                ws.send("PING")

        except Exception as exc:
            log(f"[CLOB HEARTBEAT ERROR] {exc}")
            return


def on_market_message(ws, message):
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
        log("[CLOB WARNING] Messaggio non JSON ignorato")
        return

    enqueue({
        "rx": time.time(),
        "ev": event,
    })


def on_market_error(ws, error):
    log(f"[CLOB ERROR] {error}")


def on_market_close(ws, close_status_code, close_msg):
    with ws_lock:
        if market_ws["app"] is ws:
            market_ws["app"] = None

        subscribed.clear()

    log(
        f"[CLOB CLOSED] {close_status_code} "
        f"{close_msg}"
    )


def market_loop():
    while not stop_event.is_set():
        app = websocket.WebSocketApp(
            WS_URL,
            on_open=on_market_open,
            on_message=on_market_message,
            on_error=on_market_error,
            on_close=on_market_close,
        )

        try:
            # Il PING testuale applicativo è gestito
            # separatamente da heartbeat_loop().
            app.run_forever()
        except Exception as exc:
            log(f"[CLOB LOOP ERROR] {exc}")

        with ws_lock:
            if market_ws["app"] is app:
                market_ws["app"] = None
            subscribed.clear()

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)


def parse_coinbase_time(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def on_coinbase_open(ws):
    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["matches"],
    }))
    log("[COINBASE] BTC-USD reference connected")


def on_coinbase_message(ws, message):
    try:
        event = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        return

    if not isinstance(event, dict):
        return

    if event.get("type") != "match":
        return

    try:
        price = float(event["price"])
        timestamp_ms = parse_coinbase_time(event["time"])
    except (KeyError, TypeError, ValueError):
        return

    enqueue({
        "rx": time.time(),
        "ref": {
            "T": timestamp_ms,
            "p": price,
            "source": "coinbase",
        },
    })


def on_coinbase_error(ws, error):
    log(f"[COINBASE ERROR] {error}")


def on_coinbase_close(ws, close_status_code, close_msg):
    log(
        f"[COINBASE CLOSED] {close_status_code} "
        f"{close_msg}"
    )


def coinbase_loop():
    while not stop_event.is_set():
        app = websocket.WebSocketApp(
            COINBASE_WS,
            on_open=on_coinbase_open,
            on_message=on_coinbase_message,
            on_error=on_coinbase_error,
            on_close=on_coinbase_close,
        )

        try:
            app.run_forever()
        except Exception as exc:
            log(f"[COINBASE LOOP ERROR] {exc}")

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)


def main():
    log("=" * 60)
    log("POLYMARKET BTC UP/DOWN 5M - PAPER RECORDER")
    log("CLOB + COINBASE BTC-USD")
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
        target=market_loop,
        name="clob-market",
        daemon=True,
    ).start()

    coinbase_loop()


if __name__ == "__main__":
    main()
