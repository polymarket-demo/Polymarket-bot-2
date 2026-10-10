
from __future__ import annotations

import json
import os
import queue
import threading
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError

import websocket

from poly5m.discover import fetch_market, window_start, WINDOW

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"

WRITE_QUEUE_SIZE = 100000


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def main():
    os.makedirs("data", exist_ok=True)

    filename = os.path.join(
        "data",
        "poly5m_" + datetime.now(timezone.utc).strftime("%Y%m%d") + ".jsonl",
    )

    stop_event = threading.Event()
    write_queue = queue.Queue(maxsize=WRITE_QUEUE_SIZE)

    market_lock = threading.Lock()
    ws_lock = threading.Lock()

    active_markets = {}
    subscribed = set()
    market_ws = {"app": None}

    dropped_writes = {"count": 0}

    # ---------------------------------------------------------
    # WRITER: scrive i dati fuori dai callback WebSocket
    # ---------------------------------------------------------

    def write(obj):
        try:
            write_queue.put_nowait(obj)
        except queue.Full:
            dropped_writes["count"] += 1

            if dropped_writes["count"] % 1000 == 1:
                print(
                    "[WRITER WARNING] Coda piena; "
                    f"record persi finora={dropped_writes['count']}"
                )

    def writer_loop():
        try:
            with open(filename, "a", encoding="utf-8", buffering=1024 * 64) as f:
                last_flush = time.monotonic()
                pending = 0

                while not stop_event.is_set() or not write_queue.empty():
                    try:
                        obj = write_queue.get(timeout=0.25)
                    except queue.Empty:
                        if pending:
                            f.flush()
                            pending = 0
                        last_flush = time.monotonic()
                        continue

                    try:
                        f.write(
                            json.dumps(
                                obj,
                                separators=(",", ":"),
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        pending += 1
                    except Exception as exc:
                        print(f"[WRITER ERROR] {exc}")
                    finally:
                        write_queue.task_done()

                    if (
                        pending >= 100
                        or time.monotonic() - last_flush >= 1.0
                    ):
                        f.flush()
                        pending = 0
                        last_flush = time.monotonic()

                f.flush()

        except Exception as exc:
            print(f"[WRITER FATAL ERROR] {exc}")

    threading.Thread(
        target=writer_loop,
        daemon=True,
        name="jsonl-writer",
    ).start()

    # ---------------------------------------------------------
    # MARKET DISCOVERY
    # ---------------------------------------------------------

    def discover_loop():
        while not stop_event.is_set():
            try:
                now = time.time()
                start = window_start(now)

                for s in (start, start + WINDOW):
                    with market_lock:
                        known = s in active_markets

                    if known:
                        continue

                    try:
                        market = fetch_market(s)
                    except HTTPError as exc:
                        print(
                            f"[DISCOVERY HTTP ERROR] "
                            f"status={exc.code} window={s}"
                        )
                        continue
                    except (URLError, TimeoutError) as exc:
                        print(f"[DISCOVERY CONNECTION ERROR] {exc}")
                        continue

                    if not market:
                        continue

                    with market_lock:
                        if s in active_markets:
                            continue
                        active_markets[s] = market

                    write({
                        "rx": int(time.time() * 1000),
                        "meta": market,
                    })

                    print(
                        f"[MARKET] {market.get('slug')} "
                        f"UP={market['up']} DOWN={market['down']}"
                    )

                    # Nuovi token: sottoscrizione incrementale.
                    subscribe_tokens(
                        [market["up"], market["down"]],
                        initial=False,
                    )

                time.sleep(5)

            except Exception as exc:
                print(f"[DISCOVERY ERROR] {exc}")
                time.sleep(3)

    # ---------------------------------------------------------
    # POLYMARKET CLOB
    # ---------------------------------------------------------

    def subscribe_tokens(tokens, initial=False):
        tokens = list({
            str(token)
            for token in tokens
            if token and str(token) not in subscribed
        })

        if not tokens:
            return

        with ws_lock:
            app = market_ws["app"]

            if app is None:
                return

            try:
                if initial:
                    payload = {
                        "assets_ids": tokens,
                        "type": "market",
                    }
                else:
                    payload = {
                        "assets_ids": tokens,
                        "operation": "subscribe",
                    }

                app.send(json.dumps(payload))
                subscribed.update(tokens)

                print(
                    f"[CLOB] Subscription requested: "
                    f"{len(tokens)} tokens"
                )

            except Exception as exc:
                print(f"[CLOB SUBSCRIBE ERROR] {exc}")

    def on_market_open(ws):
        print("[CLOB] Connected")

        with ws_lock:
            market_ws["app"] = ws

        with market_lock:
            tokens = [
                token
                for market in active_markets.values()
                for token in (market["up"], market["down"])
            ]

        subscribe_tokens(tokens, initial=True)

    def on_market_message(ws, message):
        if not isinstance(message, str) or not message.strip():
            return

        message = message.strip()

        if message == "PING":
            try:
                ws.send("PONG")
            except Exception as exc:
                print(f"[CLOB PONG ERROR] {exc}")
            return

        if message == "PONG":
            return

        try:
            payload = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            # Ignora messaggi di controllo non JSON.
            return

        messages = payload if isinstance(payload, list) else [payload]

        rx = int(time.time() * 1000)

        for event in messages:
            if not isinstance(event, dict):
                continue

            write({
                "rx": rx,
                "ev": event,
            })

    def on_market_error(ws, error):
        print(f"[CLOB ERROR] {error}")

    def on_market_close(ws, code, message):
        with ws_lock:
            if market_ws["app"] is ws:
                market_ws["app"] = None
            subscribed.clear()

        print(f"[CLOB CLOSED] {code} {message}")

    def market_loop():
        while not stop_event.is_set():
            try:
                app = websocket.WebSocketApp(
                    WS_URL,
                    on_open=on_market_open,
                    on_message=on_market_message,
                    on_error=on_market_error,
                    on_close=on_market_close,
                )

                app.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                )

            except Exception as exc:
                print(f"[CLOB LOOP ERROR] {exc}")

            if not stop_event.is_set():
                time.sleep(3)

    # ---------------------------------------------------------
    # COINBASE BTC-USD REFERENCE FEED
    # ---------------------------------------------------------

    def on_coinbase_open(ws):
        ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["matches"],
        }))

        print("[COINBASE] BTC-USD reference connected")

    def on_coinbase_message(ws, message):
        if not isinstance(message, str) or not message.strip():
            return

        try:
            trade = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            return

        if trade.get("type") != "match":
            return

        price = trade.get("price")
        if price is None:
            return

        timestamp_ms = int(time.time() * 1000)
        trade_time = trade.get("time")

        if trade_time:
            try:
                parsed = datetime.fromisoformat(
                    trade_time.replace("Z", "+00:00")
                )
                timestamp_ms = int(parsed.timestamp() * 1000)
            except (ValueError, TypeError):
                pass

        write({
            "rx": int(time.time() * 1000),
            "ref": {
                "T": timestamp_ms,
                "p": float(price),
                "source": "coinbase",
            },
        })

    def on_coinbase_error(ws, error):
        print(f"[COINBASE ERROR] {error}")

    def on_coinbase_close(ws, code, message):
        print(f"[COINBASE CLOSED] {code} {message}")

    # ---------------------------------------------------------
    # START
    # ---------------------------------------------------------

    threading.Thread(
        target=discover_loop,
        daemon=True,
        name="market-discovery",
    ).start()

    threading.Thread(
        target=market_loop,
        daemon=True,
        name="clob-reader",
    ).start()

    while not stop_event.is_set():
        try:
            app = websocket.WebSocketApp(
                COINBASE_WS,
                on_open=on_coinbase_open,
                on_message=on_coinbase_message,
                on_error=on_coinbase_error,
                on_close=on_coinbase_close,
            )

            app.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:
            print(f"[COINBASE LOOP ERROR] {exc}")

        if not stop_event.is_set():
            time.sleep(3)


if __name__ == "__main__":
    main()
