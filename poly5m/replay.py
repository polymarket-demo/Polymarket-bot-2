
"""Legge file JSONL e restituisce gli eventi ordinati per finestra di 5 minuti."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def _market_start(meta):
    """Compatibilità tra lo schema vecchio e quello nuovo."""
    value = meta.get("start", meta.get("market_start"))

    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _market_tokens(meta):
    """Restituisce gli ID token UP e DOWN dai due schemi."""
    up = meta.get("up", meta.get("up_token_id"))
    down = meta.get("down", meta.get("down_token_id"))

    return (
        str(up) if up is not None else None,
        str(down) if down is not None else None,
    )


def event_tokens(ev):
    """Estrae tutti gli asset ID riconoscibili da un evento CLOB."""
    tokens = set()

    asset_id = ev.get("asset_id")
    if asset_id is not None:
        tokens.add(str(asset_id))

    # Formato standard dei messaggi price_change.
    changes = ev.get("price_changes")
    if isinstance(changes, list):
        for change in changes:
            if isinstance(change, dict):
                token = change.get("asset_id")
                if token is not None:
                    tokens.add(str(token))

    # Formati alternativi: snapshot o messaggio di sottoscrizione.
    for key in ("assets_ids", "asset_ids"):
        values = ev.get(key)
        if isinstance(values, list):
            tokens.update(str(value) for value in values if value is not None)

    return sorted(tokens)


def event_ts(ev, rx):
    """
    Restituisce il timestamp dell'evento in millisecondi, se presente.
    Altrimenti usa rx, che nel JSONL del recorder è in secondi.
    """
    value = ev.get("timestamp")

    if value is None:
        return rx

    try:
        return int(value)
    except (TypeError, ValueError):
        return rx


def _read_json_lines(path):
    """Legge le righe JSON valide senza interrompersi su righe corrotte."""
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            if isinstance(obj, dict):
                yield obj


def load_windows(paths):
    """
    Restituisce [(metadata, eventi), ...], ordinati per finestra.

    Formato record supportato:
        {"rx": ..., "meta": {...}}
        {"rx": ..., "ev": {...}}

    I metadati possono usare:
        start/up/down
    oppure:
        market_start/up_token_id/down_token_id
    """
    metas = {}
    tok2start = {}
    events = {}

    # Prima passata: carica tutti i metadati.
    # In questo modo gli eventi restano associabili anche se i file
    # contengono record provenienti da più finestre.
    for path in paths:
        for obj in _read_json_lines(path):
            meta = obj.get("meta")

            if not isinstance(meta, dict):
                continue

            start = _market_start(meta)
            up, down = _market_tokens(meta)

            if start is None or up is None or down is None:
                continue

            normalized = dict(meta)
            normalized["start"] = start
            normalized["up"] = up
            normalized["down"] = down

            metas[start] = normalized
            tok2start[up] = start
            tok2start[down] = start

    # Seconda passata: associa gli eventi ai token dei mercati.
    for path in paths:
        for obj in _read_json_lines(path):
            ev = obj.get("ev")

            if not isinstance(ev, dict):
                continue

            rx = obj.get("rx", 0)

            try:
                rx = float(rx)
            except (TypeError, ValueError):
                continue

            starts = {
                tok2start[token]
                for token in event_tokens(ev)
                if token in tok2start
            }

            # Un evento con un token noto appartiene alla relativa finestra.
            # Se contiene più token di finestre diverse, viene associato
            # a ciascuna finestra riconosciuta.
            for start in starts:
                events.setdefault(start, []).append(
                    (event_ts(ev, rx), rx, ev)
                )

    output = []

    for start in sorted(metas):
        ordered_events = sorted(
            events.get(start, []),
            key=lambda item: (item[0], item[1]),
        )
        output.append((metas[start], ordered_events))

    return output


def load_refs(paths):
    """Restituisce una serie ordinata di (timestamp_ms, prezzo BTC)."""
    output = []

    for path in paths:
        for obj in _read_json_lines(path):
            ref = obj.get("ref")

            if not isinstance(ref, dict):
                continue

            try:
                timestamp = int(ref.get("T") or float(obj["rx"]) * 1000)
                price = float(ref["p"])
            except (KeyError, TypeError, ValueError, OverflowError):
                continue

            if price <= 0:
                continue

            output.append((timestamp, price))

    output.sort(key=lambda item: item[0])
    return output
