from __future__ import annotations
import json
import os
import threading
import time
from datetime import datetime, timezone
import websocket
from poly5m.discover import fetch_market, window_start, WINDOW
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
def utc_now():
    return datetime.now(timezone.utc).isoformat()
def main():
    os.makedirs("data", exist_ok=True)
    filename = os.path.join(
        "data",
        "poly5m_" + datetime.now(timezone.utc).strftime("%Y%m%d") + ".jsonl",
    )
    lock = threading.Lock()
    subscribed = set()
    active_markets = {}
    stop_event = threading.Event()
    def write(obj):
        line = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
        with lock:
            with open(filename, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    def discover_loop():
        while not stop_event.is_set():
            try:
                now = time.time()
                start = window_start(now)
                for s in (start, start + WINDOW):
                    if s in active_markets:
                        continue
                    market = fetch_market(s)
                    if not market:
                        continue
                    active_markets[s] = market
                    write({"rx": int(time.time() * 1000), "meta": market})
                    print(
                        f"[MARKET] {market['slug']} "
                        f"UP={market['up']} DOWN={market['down']}"
                    )
                time.sleep(5)
            except Exception as exc:
                print(f"[DISCOVERY ERROR] {exc}")
                time.sleep(3)
    def on_market_open(ws):
        print("[CLOB] Connected")
        with lock:
            tokens = []
            for market in active_markets.values():
                tokens.extend([market["up"], market["down"]])
            tokens = list(set(tokens))
        if tokens:
            ws.send(json.dumps({
                "assets_ids": tokens,
                "type": "market",
                "custom_feature_enabled": True,
            }))
            subscribed.update(tokens)
            print(f"[CLOB] Subscribed to {len(tokens)} tokens")
    def on_market_message(ws, message):
        try:
            payload = json.loads(message)
            messages = payload if isinstance(payload, list) else [payload]
            for event in messages:
                if not isinstance(event, dict):
                    continue
                rx = int(time.time() * 1000)
                write({"rx": rx, "ev": event})
                changes = event.get("price_changes", [])
                for change in changes:
                    token = str(change.get("asset_id", ""))
                    if token and token not in subscribed:
                        subscribed.add(token)
                        ws.send(json.dumps({
                            "assets_ids": [token],
                            "operation": "subscribe",
                        }))
        except Exception as exc:
            print(f"[CLOB MESSAGE ERROR] {exc}")
    def on_market_error(ws, error):
        print(f"[CLOB ERROR] {error}")
    def on_market_close(ws, code, message):
        print(f"[CLOB CLOSED] {code} {message}")
    def on_binance_message(ws, message):
        try:
            trade = json.loads(message)
            write({
                "rx": int(time.time() * 1000),
                "ref": {
                    "T": int(trade.get("T", time.time() * 1000)),
                    "p": float(trade["p"]),
                },
            })
        except Exception as exc:
            print(f"[BINANCE MESSAGE ERROR] {exc}")
    def on_binance_open(ws):
        print("[BINANCE] BTCUSDT reference connected")
    def on_binance_error(ws, error):
        print(f"[BINANCE ERROR] {error}")
    threading.Thread(target=discover_loop, daemon=True).start()
    def run_forever():
        while not stop_event.is_set():
            try:
                app = websocket.WebSocketApp(
                    WS_URL,
                    on_open=on_market_open,
                    on_message=on_market_message,
                    on_error=on_market_error,
                    on_close=on_market_close,
                )
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                print(f"[CLOB LOOP ERROR] {exc}")
            time.sleep(3)
    threading.Thread(target=run_forever, daemon=True).start()
    while True:
        try:
            app = websocket.WebSocketApp(
                BINANCE_WS,
                on_open=on_binance_open,
                on_message=on_binance_message,
                on_error=on_binance_error,
            )
            app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            print(f"[BINANCE LOOP ERROR] {exc}")
        time.sleep(3)
if __name__ == "__main__":
    main()
