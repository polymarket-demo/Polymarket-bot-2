
"""Legge file JSONL e restituisce gli eventi ordinati per finestra di 5 minuti."""

from __future__ import annotations

import json


def event_tokens(ev):
    if "asset_id" in ev:
        return [str(ev["asset_id"])]

    if "price_changes" in ev:
        return list({
            str(c["asset_id"])
            for c in ev["price_changes"]
        })

    return [
        str(a)
        for a in (ev.get("assets_ids") or ev.get("asset_ids") or [])
    ]


def event_ts(ev, rx):
    try:
        return int(ev["timestamp"])
    except (KeyError, TypeError, ValueError):
        return rx


def load_windows(paths):
    metas, tok2start, events = {}, {}, {}

    for p in paths:
        with open(p) as f:
            for line in f:
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if "meta" in o:
                    m = o["meta"]
                    metas[m["start"]] = m
                    tok2start[str(m["up"])] = m["start"]
                    tok2start[str(m["down"])] = m["start"]

                elif "ev" in o:
                    ev = o["ev"]
                    st = next(
                        (
                            tok2start[t]
                            for t in event_tokens(ev)
                            if t in tok2start
                        ),
                        None,
                    )

                    if st is not None:
                        events.setdefault(st, []).append(
                            (event_ts(ev, o["rx"]), o["rx"], ev)
                        )

    out = []

    for st in sorted(metas):
        evs = sorted(
            events.get(st, []),
            key=lambda x: (x[0], x[1]),
        )
        out.append((metas[st], evs))

    return out


def load_refs(paths):
    """Restituisce una serie ordinata di (timestamp_ms, prezzo)."""
    out = []

    for p in paths:
        with open(p) as f:
            for line in f:
                if '"ref"' not in line:
                    continue

                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue

                r = o.get("ref")

                if r:
                    out.append((
                        int(r.get("T") or o["rx"]),
                        float(r["p"]),
                    ))

    out.sort()
    return out
