
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError

import websocket

from poly5m.discover import fetch_market, window_start, WINDOW

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def main():
    os.makedirs("data", exist_ok=True)

    filename = os.path.join(
        "data",
        "poly5m_" + datetime.now(timezone.utc).strftime("%Y%m%d") + ".jsonl",
    )

    file_lock = threading.Lock()
    market_lock = threading.Lock()
    ws_lock = threading.Lock()

    active_markets = {}
    subscribed = set()
    market_ws = {"app": None}
    stop_event = threading.Event()

    def write(obj):
        line = json.dumps(
            obj,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        with file_lock:
            with open(filename, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def subscribe_tokens(tokens):
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
                app.send(json.dumps({
                    "assets_ids": tokens,
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                subscribed.update(tokens)
                print(f"[CLOB] Subscription requested: {len(tokens)} tokens")

            except Exception as exc:
                print(f"[CLOB SUBSCRIBE ERROR] {exc}")

    def discover_loop():
        while not stop_event.is_set():
            try:
                now = time.time()
                start = window_start(now)

                for s in (start, start + WINDOW):
                    with market_lock:
                        already_known = s in active_markets

                    if already_known:
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

                    subscribe_tokens([market["up"], market["down"]])

                time.sleep(5)

            except Exception as exc:
                print(f"[DISCOVERY ERROR] {exc}")
                time.sleep(3)

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

        subscribe_tokens(tokens)

    def on_market_message(ws, message):
        try:
            if message == "PONG":
                return

            payload = json.loads(message)
            messages = payload if isinstance(payload, list) else [payload]

            for event in messages:
                if not isinstance(event, dict):
                    continue

                rx = int(time.time() * 1000)
                write({
                    "rx": rx,
                    "ev": event,
                })

        except Exception as exc:
            print(f"[CLOB MESSAGE ERROR] {exc}")

    def on_market_error(ws, error):
        print(f"[CLOB ERROR] {error}")

    def on_market_close(ws, code, message):
        with ws_lock:
            if market_ws["app"] is ws:
                market_ws["app"] = None

        subscribed.clear()
        print(f"[CLOB CLOSED] {code} {message}")

    def on_coinbase_open(ws):
        ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["matches"],
        }))
        print("[COINBASE] BTC-USD reference connected")

    def on_coinbase_message(ws, message):
        try:
            trade = json.loads(message)

            if trade.get("type") != "match":
                return

            price = trade.get("price")
            if price is None:
                return

            trade_time = trade.get("time")
            timestamp_ms = int(time.time() * 1000)

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

        except Exception as exc:
            print(f"[COINBASE MESSAGE ERROR] {exc}")

    def on_coinbase_error(ws, error):
        print(f"[COINBASE ERROR] {error}")

    def on_coinbase_close(ws, code, message):
        print(f"[COINBASE CLOSED] {code} {message}")

    threading.Thread(
        target=discover_loop,
        daemon=True,
    ).start()

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

            time.sleep(3)

    threading.Thread(
        target=market_loop,
        daemon=True,
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

        time.sleep(3)


if __name__ == "__main__":
    main()
