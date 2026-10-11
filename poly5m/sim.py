
"""Simulatore PAPER di due strategie sullo stesso replay.
Nessun ordine reale.

A) ARBITRAGGIO TAKER
Compra Up e Down agli ask quando il profitto netto stimato supera
la soglia configurata.

B) LADDER MAKER
Bid passivi su entrambi i lati. Il modello richiede un trade SELL
a prezzo strettamente inferiore al nostro bid.
Non modella la priorità reale in coda: P&L solo indicativo.
"""
from __future__ import annotations

from dataclasses import dataclass

from .book import Book
from .fees import taker_fee, FEE_RATE


@dataclass
class SimConfig:
    fee_rate: float = FEE_RATE

    # Arbitraggio taker
    arb_margin: float = 0.005
    arb_persist_ms: int = 500
    arb_shares_cap: float = 20.0
    arb_from_s: float = 5.0
    arb_until_s: float = 285.0
    arb_max_observation_gap_ms: int = 5000

    # Ladder maker
    ladder: tuple = (0.46, 0.45, 0.44, 0.43)
    ladder_size: float = 5.0
    quote_from_s: float = 5.0
    cancel_before_end_s: float = 60.0
    fill_on_equal: bool = False  # mantenuto per compatibilità


def simulate_window(meta, events, cfg: SimConfig):
    up = str(meta["up"])
    down = str(meta["down"])
    start = float(meta["start"])
    end = float(meta["end"])

    t0 = start * 1000.0
    duration_s = end - start

    books = {up: Book(), down: Book()}

    state = {
        "orders": [],
        "placed": False,
        "cancelled": False,
    }

    inv = {up: 0.0, down: 0.0}
    cost_by_token = {up: 0.0, down: 0.0}
    maker_cost = 0.0
    maker_fills = 0

    arb = {
        "shares": 0.0,
        "pnl": 0.0,
        "events": 0,
        "since": None,
        "best_sum": None,
        "below1_ms": 0,
        "below_net_ms": 0,
        "last_ts": None,
        "was_below1": False,
        "was_net_positive": False,
    }

    winner = None

    def reset_arb_persistence():
        arb["since"] = None
        arb["last_ts"] = None
        arb["was_below1"] = False
        arb["was_net_positive"] = False

    def evaluate(ts):
        """Valuta il book una volta per timestamp, dopo tutti gli eventi
        dello stesso timestamp."""
        t = (ts - t0) / 1000.0

        ba_u = books[up].best_ask()
        ba_d = books[down].best_ask()

        in_arb_window = cfg.arb_from_s <= t <= cfg.arb_until_s

        valid_books = (
            ba_u is not None
            and ba_d is not None
            and ba_u[0] > 0
            and ba_d[0] > 0
            and ba_u[1] > 0
            and ba_d[1] > 0
        )

        # ---------------------------------------------------------
        # A) ARBITRAGGIO TAKER
        # ---------------------------------------------------------
        if in_arb_window and valid_books:
            ask_u = float(ba_u[0])
            qty_u = float(ba_u[1])
            ask_d = float(ba_d[0])
            qty_d = float(ba_d[1])

            ask_sum = ask_u + ask_d

            if (
                arb["best_sum"] is None
                or ask_sum < arb["best_sum"]
            ):
                arb["best_sum"] = ask_sum

            gross_edge = 1.0 - ask_sum
            fee_per_pair = (
                taker_fee(1.0, ask_u, cfg.fee_rate)
                + taker_fee(1.0, ask_d, cfg.fee_rate)
            )
            net_per_pair = gross_edge - fee_per_pair

            previous_ts = arb["last_ts"]
            gap_ok = (
                previous_ts is not None
                and 0 <= ts - previous_ts
                <= cfg.arb_max_observation_gap_ms
            )

            if gap_ok:
                dt = ts - previous_ts

                if arb["was_below1"] and ask_sum < 1.0:
                    arb["below1_ms"] += dt

                if (
                    arb["was_net_positive"]
                    and net_per_pair >= cfg.arb_margin
                ):
                    arb["below_net_ms"] += dt

            arb["was_below1"] = ask_sum < 1.0
            arb["was_net_positive"] = (
                net_per_pair >= cfg.arb_margin
            )

            if net_per_pair >= cfg.arb_margin:
                if arb["since"] is None or not gap_ok:
                    arb["since"] = ts

                persistence_ok = (
                    ts - arb["since"] >= cfg.arb_persist_ms
                )

                remaining_cap = (
                    cfg.arb_shares_cap - arb["shares"]
                )

                if persistence_ok and remaining_cap > 1e-9:
                    q = min(qty_u, qty_d, remaining_cap)

                    if q > 1e-9:
                        arb["shares"] += q
                        arb["pnl"] += q * net_per_pair
                        arb["events"] += 1

                        # Nuova persistenza prima della prossima
                        # contabilizzazione.
                        arb["since"] = ts
            else:
                arb["since"] = None

            arb["last_ts"] = ts

        else:
            reset_arb_persistence()

        # ---------------------------------------------------------
        # B) LADDER MAKER
        # ---------------------------------------------------------
        if (
            not state["placed"]
            and t >= cfg.quote_from_s
            and valid_books
        ):
            for tok in (up, down):
                ask = books[tok].best_ask()

                if ask is None:
                    continue

                ask_price = float(ask[0])

                for price in cfg.ladder:
                    p = float(price)

                    # Post-only: mai attraversare lo spread.
                    if p > 0 and p < ask_price:
                        state["orders"].append({
                            "tok": tok,
                            "price": p,
                            "rem": max(
                                0.0, float(cfg.ladder_size)
                            ),
                        })

            state["placed"] = True

        if (
            state["placed"]
            and not state["cancelled"]
            and t >= duration_s - cfg.cancel_before_end_s
        ):
            state["orders"] = []
            state["cancelled"] = True

    # -------------------------------------------------------------
    # REPLAY EVENTI
    # -------------------------------------------------------------
    dirty = False
    last_ts = None

    for ts, rx, ev in events:
        try:
            ts = float(ts)
        except (TypeError, ValueError):
            continue

        # Valuta il timestamp precedente prima di passare al nuovo.
        if dirty and last_ts is not None and ts != last_ts:
            evaluate(last_ts)
            dirty = False

        last_ts = ts
        event_type = ev.get("event_type")

        if event_type == "book":
            tok = str(ev.get("asset_id", ""))

            if tok in books:
                books[tok].snapshot(
                    ev.get("bids", []),
                    ev.get("asks", []),
                )
                dirty = True

        elif event_type == "price_change":
            for change in ev.get("price_changes", []):
                tok = str(change.get("asset_id", ""))

                if tok not in books:
                    continue

                books[tok].change(
                    change.get("side"),
                    change.get("price"),
                    change.get("size"),
                )
                dirty = True

        elif event_type == "last_trade_price":
            tok = str(ev.get("asset_id", ""))

            if tok not in books or not state["orders"]:
                continue

            try:
                trade_price = float(ev["price"])
                trade_size = max(
                    0.0, float(ev.get("size") or 0)
                )
            except (TypeError, ValueError, KeyError):
                continue

            # ASSUNZIONE: side=SELL identifica una vendita aggressiva
            # che può colpire i nostri bid. Va verificata sul feed.
            side = str(ev.get("side", "")).upper()

            if side == "SELL" and trade_size > 0:
                candidates = sorted(
                    (
                        order
                        for order in state["orders"]
                        if order["tok"] == tok
                    ),
                    key=lambda order: -order["price"],
                )

                remaining_trade_size = trade_size

                for order in candidates:
                    # Fill solo se il trade avviene sotto il bid.
                    if not trade_price < order["price"]:
                        continue

                    if (
                        order["rem"] <= 1e-9
                        or remaining_trade_size <= 1e-9
                    ):
                        continue

                    q = min(
                        order["rem"],
                        remaining_trade_size,
                    )

                    if q <= 1e-9:
                        continue

                    order["rem"] -= q
                    remaining_trade_size -= q

                    inv[tok] += q
                    fill_cost = q * order["price"]
                    maker_cost += fill_cost
                    cost_by_token[tok] += fill_cost
                    maker_fills += 1

            state["orders"] = [
                order
                for order in state["orders"]
                if order["rem"] > 1e-9
            ]

        elif event_type == "market_resolved":
            resolved_token = ev.get("winning_asset_id")

            if resolved_token is not None:
                resolved_token = str(resolved_token)

                if resolved_token in (up, down):
                    winner = resolved_token

    if dirty and last_ts is not None:
        evaluate(last_ts)

    # -------------------------------------------------------------
    # RISULTATI
    # -------------------------------------------------------------
    up_qty = inv[up]
    down_qty = inv[down]
    resolved = winner in (up, down)

    result = {
        "start": start,
        "resolved": resolved,

        # Taker: stima al netto delle fee configurate.
        "arb_shares": round(arb["shares"], 6),
        "arb_pnl": round(arb["pnl"], 6),
        "arb_events": arb["events"],
        "min_ask_sum": (
            round(arb["best_sum"], 6)
            if arb["best_sum"] is not None
            else None
        ),
        "ms_ask_sum_below_1": arb["below1_ms"],
        "ms_net_opportunity": arb["below_net_ms"],

        # Maker: risultato indicativo, coda non modellata.
        "fills": maker_fills,
        "cost": round(maker_cost, 6),
        "up_qty": round(up_qty, 6),
        "down_qty": round(down_qty, 6),
        "pairs": round(min(up_qty, down_qty), 6),
        "leftover_shares": round(
            abs(up_qty - down_qty), 6
        ),
        "maker_pnl": None,
        "pair_pnl": None,
        "leftover_pnl": None,
        "maker_pnl_status": (
            "ESTIMATE_QUEUE_NOT_MODELED"
            if resolved
            else "UNRESOLVED"
        ),
    }

    if resolved:
        # Una share vincente paga 1; una perdente paga 0.
        gross_redemption = inv[winner]
        maker_pnl = gross_redemption - maker_cost
        pairs = min(up_qty, down_qty)

        # Ripartizione contabile usando il costo medio per token.
        avg_up_cost = (
            cost_by_token[up] / up_qty
            if up_qty > 0
            else 0.0
        )
        avg_down_cost = (
            cost_by_token[down] / down_qty
            if down_qty > 0
            else 0.0
        )

        pair_pnl = pairs * (
            1.0 - avg_up_cost - avg_down_cost
        )
        leftover_pnl = maker_pnl - pair_pnl

        result.update({
            "maker_pnl": round(maker_pnl, 6),
            "pair_pnl": round(pair_pnl, 6),
            "leftover_pnl": round(leftover_pnl, 6),
        })

    return result


def simulate_all(windows, cfg: SimConfig | None = None):
    """Simula tutte le finestre non vuote."""
    cfg = cfg or SimConfig()
    return [
        simulate_window(meta, events, cfg)
        for meta, events in windows
        if events
    ]
