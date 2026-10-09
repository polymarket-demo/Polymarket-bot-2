"""Registratore dati (solo lettura, endpoint pubblici, nessuna chiave):
  pip install websocket-client
  python -m poly5m.record --out data
Salva ogni evento del book in data/poly5m_YYYYMMDD.jsonl (una riga = un evento, con orario locale 'rx').
Lascialo girare piu' giorni: serve per misurare fill reali e spread, non per tradare."""
from __future__ import annotations
import argparse, json, os, threading, time
from datetime import datetime, timezone
from .discover import fetch_market, window_start, WINDOW

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
# Prezzo di riferimento. NB: Polymarket risolve con il proprio oracolo (Chainlink), non con Binance:
# Binance e' solo un proxy. Se sei in una zona dove binance.com e' bloccato usa --ref-url con un altro stream.
REF_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"


class Recorder:
    def __init__(self, outdir, ref_url=REF_URL, ref_ms=250):
        self.ref_url, self.ref_ms, self._ref_last = ref_url, ref_ms, 0
        self.outdir, self.ws, self.open = outdir, None, False
        self.markets, self.subscribed, self.retry_at = {}, set(), {}
        self.lock, self.fh, self.fday = threading.Lock(), None, None
        os.makedirs(outdir, exist_ok=True)

    # ---- scrittura
    def _write(self, obj):
        now = datetime.now(timezone.utc); day = now.strftime("%Y%m%d")
        with self.lock:
            if day != self.fday:
                if self.fh: self.fh.close()
                self.fh = open(os.path.join(self.outdir, f"poly5m_{day}.jsonl"), "a", buffering=1)
                self.fday = day
                for m in self.markets.values():          # meta in ogni file giornaliero
                    if m["end"] > time.time() - 600: self.fh.write(json.dumps({"rx": int(time.time()*1000), "meta": m}) + "\n")
            self.fh.write(json.dumps(obj, separators=(",", ":")) + "\n")

    # ---- websocket
    def _on_open(self, ws):
        self.open = True; self.subscribed = set()
        toks = [t for m in self.markets.values() for t in (m["up"], m["down"])]
        if toks:
            ws.send(json.dumps({"assets_ids": toks, "type": "market", "custom_feature_enabled": True}))
            self.subscribed.update(toks)
        print(f"[rec] websocket connesso ({len(toks)} token)")

    def _on_message(self, ws, raw):
        if raw in ("PONG", "pong"): return
        rx = int(time.time() * 1000)
        try: msg = json.loads(raw)
        except json.JSONDecodeError: return
        for ev in (msg if isinstance(msg, list) else [msg]):
            if isinstance(ev, dict): self._write({"rx": rx, "ev": ev})

    def _ws_thread(self):
        import websocket
        while True:
            try:
                self.ws = websocket.WebSocketApp(
                    WS_URL, on_open=self._on_open, on_message=self._on_message,
                    on_error=lambda w, e: print(f"[rec] errore ws: {e}"),
                    on_close=lambda w, c, m: print(f"[rec] ws chiuso {c} {m}"))
                self.ws.run_forever()
            except Exception as e:
                print(f"[rec] eccezione ws: {e}")
            self.open = False
            time.sleep(5)

    def _ref_thread(self):
        """Registra il prezzo BTC di riferimento (al massimo 1 riga ogni ref_ms) come {"ref": {...}}."""
        import websocket
        def on_message(ws, raw):
            try: m = json.loads(raw); price = float(m["p"]); t = int(m.get("T") or m.get("E"))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError): return
            rx = int(time.time() * 1000)
            if rx - self._ref_last >= self.ref_ms:
                self._ref_last = rx
                self._write({"rx": rx, "ref": {"src": "binance", "p": price, "T": t}})
        while True:
            try:
                websocket.WebSocketApp(self.ref_url, on_message=on_message,
                    on_open=lambda w: print("[rec] riferimento BTC connesso"),
                    on_error=lambda w, e: print(f"[rec] errore riferimento: {e}"),
                    on_close=lambda w, c, m: print(f"[rec] riferimento chiuso {c}")).run_forever()
            except Exception as e:
                print(f"[rec] eccezione riferimento: {e}")
            time.sleep(5)

    def _ping_thread(self):
        while True:
            time.sleep(10)
            try:
                if self.open and self.ws: self.ws.send("PING")
            except Exception: pass

    # ---- ciclo principale: scopre le finestre in arrivo
    def run(self):
        threading.Thread(target=self._ws_thread, daemon=True).start()
        threading.Thread(target=self._ping_thread, daemon=True).start()
        if self.ref_url: threading.Thread(target=self._ref_thread, daemon=True).start()
        while True:
            now = time.time(); cur = window_start(now)
            for st in (cur, cur + WINDOW, cur + 2 * WINDOW):
                if st in self.markets or self.retry_at.get(st, 0) > now: continue
                try:
                    m = fetch_market(st)
                except Exception as e:
                    print(f"[rec] discover {st}: {e}"); m = None
                if m is None:
                    self.retry_at[st] = now + 5; continue
                self.markets[st] = m
                self._write({"rx": int(now * 1000), "meta": m})
                print(f"[rec] mercato {m['slug']} fees={m['fees_enabled']}")
                if self.open and self.ws:
                    try:
                        self.ws.send(json.dumps({"assets_ids": [m["up"], m["down"]], "operation": "subscribe"}))
                        self.subscribed.update([m["up"], m["down"]])
                    except Exception as e: print(f"[rec] subscribe: {e}")
            for st in [s for s, m in self.markets.items() if m["end"] < now - 120]:   # pulizia
                m = self.markets.pop(st)
                try:
                    if self.open and self.ws:
                        self.ws.send(json.dumps({"assets_ids": [m["up"], m["down"]], "operation": "unsubscribe"}))
                except Exception: pass
            time.sleep(2)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--out", default="data")
    ap.add_argument("--ref-url", default=REF_URL, help="stream prezzo BTC ('' per disattivare)")
    ap.add_argument("--ref-ms", type=int, default=250, help="intervallo minimo tra due righe di riferimento")
    a = ap.parse_args()
    Recorder(a.out, a.ref_url, a.ref_ms).run()


if __name__ == "__main__":
    main()
