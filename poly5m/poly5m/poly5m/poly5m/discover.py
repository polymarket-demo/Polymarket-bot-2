
from __future__ import annotations

import json
import urllib.request
import urllib.parse

GAMMA = "https://gamma-api.polymarket.com/markets"
SLUG_FMT = "btc-updown-5m-{start}"
WINDOW = 300


def window_start(ts: float) -> int:
    t = int(ts)
    return t - t % WINDOW


def parse_market(m: dict, start: int) -> dict | None:
    def lst(v):
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                return None
        return v if isinstance(v, list) else None

    outs = lst(m.get("outcomes"))
    toks = lst(m.get("clobTokenIds"))

    if not outs or not toks or len(outs) != len(toks) or len(toks) != 2:
        return None

    by = {str(o).strip().lower(): str(t) for o, t in zip(outs, toks)}

    if "up" not in by or "down" not in by:
        return None

    return {
        "start": start,
        "end": start + WINDOW,
        "up": by["up"],
        "down": by["down"],
        "condition": m.get("conditionId"),
        "slug": m.get("slug"),
        "fees_enabled": m.get("feesEnabled"),
    }


def fetch_market(start: int, timeout=10) -> dict | None:
    slug = SLUG_FMT.format(start=start)
    url = GAMMA + "?" + urllib.parse.urlencode({"slug": slug})

    with urllib.request.urlopen(url, timeout=timeout) as r:
        data = json.loads(r.read())

    if not data:
        return None

    return parse_market(data[0] if isinstance(data, list) else data, start)
