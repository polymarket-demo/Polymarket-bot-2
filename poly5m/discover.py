from __future__ import annotations
import json
import urllib.parse
import urllib.request
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
    outcomes = lst(m.get("outcomes"))
    tokens = lst(m.get("clobTokenIds"))
    if (
        not outcomes
        or not tokens
        or len(outcomes) != len(tokens)
        or len(tokens) != 2
    ):
        return None
    by = {
        str(outcome).strip().lower(): str(token)
        for outcome, token in zip(outcomes, tokens)
    }
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
def fetch_market(start: int, timeout: int = 10) -> dict | None:
    slug = SLUG_FMT.format(start=start)
    url = GAMMA + "?" + urllib.parse.urlencode({"slug": slug})
    with urllib.request.urlopen(url, timeout=timeout) as response:
        data = json.loads(response.read())
    if not data:
        return None
    market = data[0] if isinstance(data, list) else data
    return parse_market(market, start)
