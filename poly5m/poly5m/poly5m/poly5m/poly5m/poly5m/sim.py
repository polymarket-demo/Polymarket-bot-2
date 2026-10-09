
"""Simulatore PAPER di due strategie sullo stesso replay (nessun ordine reale).

A) ARBITRAGGIO TAKER: compra Up e Down agli ask quando 1 - ask_up - ask_down - fee > margine.
   Misura quante volte esiste, per quanto dura, e quanto vale DOPO le commissioni.
B) LADDER MAKER: bid passivi su entrambi i lati. Ipotesi PESSIMISTICHE:
   - un bid si riempie solo se un trade avviene a prezzo STRETTAMENTE sotto il nostro bid (priorita' di coda ignorata);
   - nessun rebate maker incluso; le coppie Up+Down valgono 1.00, il resto si regola a 1 o 0 alla risoluzione.
"""
from __future__ import annotations

from dataclasses import dataclass

from .book import Book
from .fees import taker_fee, FEE_RATE


@dataclass
class SimConfig:
    fee_rate: float = FEE_RATE
    arb_margin: float = 0.005
    arb_persist_ms: int = 500
    arb_shares_cap: float = 20.0
    arb_from_s: float = 5.0
    arb_until_s: float = 285.0
    ladder: tuple = (0.46, 0.45, 0.44, 0.43)
    ladder_size: float = 5.0
    quote_from_s: float = 5.0
    cancel_before_end_s: float = 60.0
    fill_on_equal: bool = False


def simulate_window(meta, events, cfg: SimConfig):
    up, down, start = str(meta["up"]), str(meta["down"]), meta["start"]
    t0 = start * 1000

    books = {up: Book(), down: Book()}
    st = dict(orders=[], placed=False, cancelled=False)

    inv = {up: 0.0, down: 0.0}
    ctok = {up: 0.0, down: 0.0}
    acc = dict(cost=0.0, fills=0)

    arb = dict(
        shares=0.0,
        pnl=0.0,
        events=0,
        since=None,
        best_sum=None,
        below1_ms=0,
        last_ts=None,
        was_below=False,
        episode_done=False,
    )

    winner = None

    def evaluate(ts):
        """Valuta il book una volta per timestamp, dopo tutti gli eventi."""
        t = (ts - t0) / 1000.0

        ba_u = books[up].best_ask()
        ba_d = books[down].best_ask()

        if ba_u and ba_d and cfg.arb_from_s <= t <= cfg.arb_until_s:
            s = ba_u[0] + ba_d[0]

            arb["best_sum"] = (
                s if arb["best_sum"] is None
                else min(arb["best_sum"], s)
            )

            net = (
                1.0 - s
                - taker_fee(1, ba_u[0], cfg.fee_rate)
                - taker_fee(1, ba_d[0], cfg.fee_rate)
            )

            if arb["was_below"] and arb["last_ts"] is not None:
                arb["below1_ms"] += ts - arb["last_ts"]

            arb["was_below"] = s < 1.0

            if net >= cfg.arb_margin:
                if arb["since"] is None:
                    arb["since"] = ts

                if (
                    not arb["episode_done"]
                    and ts - arb["since"] >= cfg.arb_persist_ms
                    and arb["shares"] < cfg.arb_shares_cap
                ):
                    q = min(
                        ba_u[1],
                        ba_d[1],
                        cfg.arb_shares_cap - arb["shares"],
                    )

                    if q > 0:
                        arb["shares"] += q
                        arb["pnl"] += q * net
                        arb["events"] += 1
                        arb["episode_done"] = True
            else:
                arb["since"] = None
                arb["episode_done"] = False

            arb["last_ts"] = ts

        if not st["placed"] and t >= cfg.quote_from_s and ba_u and ba_d:
            for tok in (up, down):
                ask = books[tok].best_ask()

                for p in cfg.ladder:
                    if ask is None or p < ask[0]:
                        st["orders"].append(
                            dict(tok=tok, price=p, rem=cfg.ladder_size)
                        )

            st["placed"] = True

        if (
            st["placed"]
            and not st["cancelled"]
            and t >= (meta["end"] - start) - cfg.cancel_before_end_s
        ):
            st["orders"] = []
            st["cancelled"] = True

    dirty = False
    last = None

    for ts, rx, ev in events:
        if dirty and ts != last:
            evaluate(last)
            dirty = False

        last = ts
        et = ev.get("event_type")

        if et == "book":
            tok = str(ev["asset_id"])

            if tok in books:
                books[tok].snapshot(
                    ev.get("bids", []),
                    ev.get("asks", []),
                )
                dirty = True

        elif et == "price_change":
            for c in ev.get("price_changes", []):
                tok = str(c["asset_id"])

                if tok in books:
                    books[tok].change(
                        c["side"],
                        c["price"],
                        c["size"],
                    )
                    dirty = True

        elif et == "last_trade_price":
            if str(ev.get("side", "")).upper() != "SELL":
                continue

            tok = str(ev.get("asset_id"))

            if tok in books and st["orders"]:
                p = float(ev["price"])
                left = float(ev.get("size") or 0)

                for o in sorted(
                    (o for o in st["orders"] if o["tok"] == tok),
                    key=lambda o: -o["price"],
                ):
                    hit = (
                        o["price"] > p
                        or (
                            cfg.fill_on_equal
                            and o["price"] == p
                        )
                    )

                    if hit and left > 0 and o["rem"] > 0:
                        q = min(o["rem"], left)
                        o["rem"] -= q
                        left -= q

                        inv[tok] += q
                        acc["cost"] += q * o["price"]
                        ctok[tok] += q * o["price"]
                        acc["fills"] += 1

                st["orders"] = [
                    o for o in st["orders"]
                    if o["rem"] > 1e-9
                ]

        elif et == "market_resolved":
            w = ev.get("winning_asset_id")

            if w is not None:
                winner = str(w)

    if dirty:
        evaluate(last)

    cost = acc["cost"]

    res = dict(
        start=start,
        resolved=winner in (up, down),
        fills=acc["fills"],
        cost=round(cost, 4),
        up_qty=inv[up],
        down_qty=inv[down],
        arb_shares=round(arb["shares"], 3),
        arb_pnl=round(arb["pnl"], 4),
        arb_events=arb["events"],
        min_ask_sum=arb["best_sum"],
        ms_ask_sum_below_1=arb["below1_ms"],
        maker_pnl=0.0,
        pair_pnl=0.0,
        leftover_pnl=0.0,
        pairs=0.0,
        leftover_shares=0.0,
    )

    if winner in (up, down):
        pairs = min(inv[up], inv[down])

        res["maker_pnl"] = round(inv[winner] - cost, 4)
        res["pairs"] = pairs

        if pairs > 0:
            res["pair_pnl"] = round(
                pairs * (
                    1.0
                    - ctok[up] / inv[up]
                    - ctok[down] / inv[down]
                ),
                4,
            )

        res["leftover_pnl"] = round(
            res["maker_pnl"] - res["pair_pnl"],
            4,
        )
        res["leftover_shares"] = abs(inv[up] - inv[down])

    return res


def simulate_all(windows, cfg: SimConfig | None = None):
    cfg = cfg or SimConfig()
    return [
        simulate_window(m, e, cfg)
        for m, e in windows
        if e
    ]
