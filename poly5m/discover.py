
from __future__ import annotations

import json
import urllib.parse
import urllib.request

GAMMA = "https://gamma-api.polymarket.com/events"
SLUG_FMT = "btc-updown-5m-{start}"
WINDOW = 300


def window_start(ts: float) -> int:
    t = int(ts)
    return t - t % WINDOW


def parse_list(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None

    return value if isinstance(value, list) else None


def parse_market(event: dict, start: int) -> dict | None:
    markets = event.get("markets")

    if isinstance(markets, str):
        try:
            markets = json.loads(markets)
        except json.JSONDecodeError:
            return None

    if not isinstance(markets, list) or not markets:
        return None

    for market in markets:
        if not isinstance(market, dict):
            continue

        outcomes = parse_list(market.get("outcomes"))
        tokens = parse_list(market.get("clobTokenIds"))

        if not outcomes or not tokens:
            continue

        if len(outcomes) != len(tokens) or len(tokens) != 2:
            continue

        by_outcome = {
            str(outcome).strip().lower(): str(token)
            for outcome, token in zip(outcomes, tokens)
        }

        if "up" not in by_outcome or "down" not in by_outcome:
            continue

        return {
            "start": start,
            "end": start + WINDOW,
            "up": by_outcome["up"],
            "down": by_outcome["down"],
            "condition": market.get("conditionId"),
            "slug": event.get("slug") or market.get("slug"),
            "fees_enabled": market.get("feesEnabled"),
        }

    return None


def fetch_market(start: int, timeout: int = 10) -> dict | None:
    slug = SLUG_FMT.format(start=start)

    query = urllib.parse.urlencode({"slug": slug})
    url = f"{GAMMA}?{query}"

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "PolymarketPaperBot/1.0",
        },
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        data = json.loads(response.read().decode("utf-8"))

    if isinstance(data, dict):
        events = [data]
    elif isinstance(data, list):
        events = data
    else:
        return None

    for event in events:
        if not isinstance(event, dict):
            continue

        result = parse_market(event, start)

        if result:
            return result

    return None
