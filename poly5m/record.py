
"""
POLYMARKET BTC UP/DOWN 5M — PAPER RECORDER
- Coinbase BTC-USD reference price
- Polymarket CLOB market WebSocket
- JSONL append-only recorder
- Automatic 5-minute market rotation
- Bounded queue, writer thread, counters and graceful shutdown
- NO ORDERS — DATA COLLECTION ONLY

Dependencies:
    pip install websockets requests

Run:
    python record.py
"""

import asyncio
import json
import logging
import os
import queue
import signal
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import websockets


# ============================================================
# CONFIGURATION
# ============================================================

DATA_DIR = os.getenv("DATA_DIR", "data")

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"

COINBASE_PRODUCT = "BTC-USD"
MARKET_PREFIX = "btc-updown-5m-"

MARKET_DURATION_SECONDS = 300
MARKET_REFRESH_SECONDS = 5

QUEUE_MAXSIZE = 100_000
FLUSH_EVERY_RECORDS = 20
WRITER_LOG_INTERVAL_SECONDS = 30

WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 20
WS_CLOSE_TIMEOUT = 10

# Do not enable trading in this recorder.
TRADING_ENABLED = False


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("poly5m")


def log(message):
    logger.info(message)


# ============================================================
# SHARED STATE
# ============================================================

record_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

stop_event = threading.Event()
stats_lock = threading.Lock()
market_lock = threading.Lock()

statistics = {
    "clob_events": 0,
    "btc_ticks": 0,
    "records_written": 0,
    "records_dropped": 0,
    "writer_errors": 0,
}

last_writer_log = 0.0

current_market = None


# ============================================================
# QUEUE / RECORDING
# ============================================================

def save_record(item):
    """
    Add one record to the writer queue without blocking WebSocket
    callbacks. If the queue is full, count the dropped record.
    """
    if not isinstance(item, dict):
        log("[RECORDER ERROR] Record must be a dictionary")
        return False

    try:
        record_queue.put_nowait(item)
        return True

    except queue.Full:
        with stats_lock:
            statistics["records_dropped"] += 1
            dropped = statistics["records_dropped"]

        if dropped == 1 or dropped % 100 == 0:
            log(f"[WRITER WARNING] records_dropped={dropped}")

        return False


def writer_loop():
    """
    Dedicated writer thread.

    Output schema:
        CLOB:     {"rx": ..., "ev": {...}}
        Coinbase: {"rx": ..., "ref": {"T": ..., "p": ..., "source": ...}}
        Market:   {"rx": ..., "meta": {...}}
    """
    global last_writer_log

    os.makedirs(DATA_DIR, exist_ok=True)

    current_day = None
    handle = None
    pending_flush = 0

    try:
        while not stop_event.is_set() or not record_queue.empty():

            try:
                item = record_queue.get(timeout=1)

            except queue.Empty:
                if handle:
                    try:
                        handle.flush()
                    except Exception as exc:
                        log(f"[WRITER FLUSH ERROR] {exc}")
                continue

            try:
                rx = float(item.get("rx", time.time()))

                day = datetime.fromtimestamp(
                    rx, timezone.utc
                ).strftime("%Y%m%d")

                # Rotate files at UTC midnight.
                if day != current_day:
                    if handle:
                        handle.flush()
                        handle.close()

                    path = os.path.join(
                        DATA_DIR,
                        f"poly5m_{day}.jsonl",
                    )

                    handle = open(
                        path,
                        "a",
                        encoding="utf-8",
                        buffering=1,
                    )

                    current_day = day
                    log(f"[WRITER] File open: {path}")

                line = json.dumps(
                    item,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )

                handle.write(line + "\n")
                pending_flush += 1

                if pending_flush >= FLUSH_EVERY_RECORDS:
                    handle.flush()
                    pending_flush = 0

                with stats_lock:
                    statistics["records_written"] += 1
                    written = statistics["records_written"]
                    dropped = statistics["records_dropped"]
                    errors = statistics["writer_errors"]
                    snapshot = dict(statistics)

                now = time.time()

                if (
                    written == 1
                    or now - last_writer_log >= WRITER_LOG_INTERVAL_SECONDS
                ):
                    handle.flush()

                    log(
                        "[WRITER] "
                        f"written={snapshot['records_written']} "
                        f"queue={record_queue.qsize()} "
                        f"dropped={dropped} "
                        f"errors={errors}"
                    )

                    last_writer_log = now

            except Exception as exc:
                with stats_lock:
                    statistics["writer_errors"] += 1

                log(f"[WRITER ERROR] {type(exc).__name__}: {exc}")

            finally:
                record_queue.task_done()

    finally:
        if handle:
            try:
                handle.flush()
                handle.close()
            except Exception as exc:
                log(f"[WRITER CLOSE ERROR] {exc}")

        log("[WRITER] Stopped")


# ============================================================
# MARKET DISCOVERY
# ============================================================

def normalize_list(value):
    """Accept API arrays represented either as lists or JSON strings."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []

    return value if isinstance(value, list) else []


def fetch_json(url, timeout=10):
    request = Request(
        url,
        headers={"User-Agent": "poly5m-paper-recorder/1.0"},
    )

    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def discover_market():
    """
    Find the BTC Up/Down 5-minute market for the current UTC time.

    Gamma API results are checked against the requested slug.
    Token IDs are matched to Up/Down using the outcome labels.
    """
    now = int(time.time())
    market_start = (now // MARKET_DURATION_SECONDS) * MARKET_DURATION_SECONDS
    slug = f"{MARKET_PREFIX}{market_start}"

    query = urlencode({"slug": slug})
    url = f"{GAMMA_API_URL}?{query}"

    result = fetch_json(url)

    if isinstance(result, dict):
        markets = [result]
    elif isinstance(result, list):
        markets = result
    else:
        markets = []

    for market in markets:
        if market.get("slug") != slug:
            continue

        if market.get("closed") is True:
            continue

        if market.get("active") is False:
            continue

        outcomes = normalize_list(market.get("outcomes"))
        token_ids = normalize_list(market.get("clobTokenIds"))

        if len(outcomes) != len(token_ids) or not token_ids:
            continue

        token_by_outcome = {}

        for outcome, token_id in zip(outcomes, token_ids):
            label = str(outcome).strip().lower()

            if label in ("up", "higher"):
                token_by_outcome["UP"] = str(token_id)
            elif label in ("down", "lower"):
                token_by_outcome["DOWN"] = str(token_id)

        if not token_by_outcome.get("UP") or not token_by_outcome.get("DOWN"):
            continue

        condition_id = market.get("conditionId") or market.get("condition_id")

        return {
            "slug": slug,
            "market_start": market_start,
            "market_end": market_start + MARKET_DURATION_SECONDS,
            "condition_id": condition_id,
            "market_id": market.get("id"),
            "question": market.get("question"),
            "up_token_id": token_by_outcome["UP"],
            "down_token_id": token_by_outcome["DOWN"],
            "outcomes": outcomes,
            "clobTokenIds": token_ids,
            "discovered_at": time.time(),
        }

    raise RuntimeError(f"Market not available or invalid: {slug}")


def set_current_market(market):
    global current_market

    with market_lock:
        current_market = dict(market)


def get_current_market():
    with market_lock:
        return dict(current_market) if current_market else None


def record_market_metadata(market):
    save_record({
        "rx": time.time(),
        "meta": market,
    })


# ============================================================
# COINBASE BTC-USD REFERENCE FEED
# ============================================================

async def coinbase_loop():
    """
    Subscribe to Coinbase BTC-USD ticker.
    Each accepted ticker is stored as a reference-price record.
    """
    while not stop_event.is_set():

        try:
            log("[BTC] Connecting to Coinbase WebSocket")

            async with websockets.connect(
                COINBASE_WS_URL,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                close_timeout=WS_CLOSE_TIMEOUT,
                max_size=4 * 1024 * 1024,
            ) as ws:

                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": [COINBASE_PRODUCT],
                    "channels": ["ticker"],
                }))

                log("[BTC] WebSocket CONNECTED")
                log("[BTC] Coinbase ticker subscribed")

                async for raw_message in ws:
                    if stop_event.is_set():
                        break

                    rx = time.time()

                    try:
                        message = json.loads(raw_message)
                    except json.JSONDecodeError:
                        log("[BTC WARNING] Invalid JSON message")
                        continue

                    if not isinstance(message, dict):
                        continue

                    if message.get("type") != "ticker":
                        continue

                    if message.get("product_id") != COINBASE_PRODUCT:
                        continue

                    try:
                        price = float(message["price"])
                    except (KeyError, TypeError, ValueError):
                        continue

                    if price <= 0:
                        continue

                    # Coinbase ticker time is an exchange timestamp.
                    timestamp_ms = int(rx * 1000)
                    exchange_time = message.get("time")

                    if exchange_time:
                        try:
                            parsed = datetime.fromisoformat(
                                exchange_time.replace("Z", "+00:00")
                            )
                            timestamp_ms = int(parsed.timestamp() * 1000)
                        except (ValueError, TypeError):
                            pass

                    with stats_lock:
                        statistics["btc_ticks"] += 1
                        tick_count = statistics["btc_ticks"]

                    save_record({
                        "rx": rx,
                        "ref": {
                            "T": timestamp_ms,
                            "p": price,
                            "source": "coinbase",
                        },
                    })

                    if tick_count == 1 or tick_count % 100 == 0:
                        log(
                            f"[BTC DATA] {COINBASE_PRODUCT}={price:.2f} "
                            f"ticks={tick_count}"
                        )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log(f"[BTC ERROR] {type(exc).__name__}: {exc}")

        if not stop_event.is_set():
            await asyncio.sleep(2)

    log("[BTC] Stopped")


# ============================================================
# POLYMARKET CLOB FEED
# ============================================================

def normalize_clob_message(parsed):
    """
    The CLOB may send either one JSON object or a list of objects.
    Return all event objects without modifying their contents.
    """
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]

    if isinstance(parsed, dict):
        return [parsed]

    return []


async def clob_market_session(market):
    """
    Connect and subscribe to both outcome token IDs for one market.
    Raw CLOB messages are preserved in the 'ev' field.
    """
    token_ids = [
        market["up_token_id"],
        market["down_token_id"],
    ]

    log(
        f"[MARKET] {market['slug']} "
        f"UP={market['up_token_id']} "
        f"DOWN={market['down_token_id']}"
    )

    async with websockets.connect(
        POLYMARKET_WS_URL,
        ping_interval=WS_PING_INTERVAL,
        ping_timeout=WS_PING_TIMEOUT,
        close_timeout=WS_CLOSE_TIMEOUT,
        max_size=8 * 1024 * 1024,
    ) as ws:

        await ws.send(json.dumps({
            "assets_ids": token_ids,
            "type": "market",
        }))

        log("[CLOB] Connected")
        log("[CLOB] Current market subscribed")

        async def rotate_check():
            """
            Close the session at the market boundary, even if no
            further CLOB messages arrive.
            """
            while not stop_event.is_set():
                if time.time() >= market["market_end"]:
                    return

                await asyncio.sleep(MARKET_REFRESH_SECONDS)

        rotation_task = asyncio.create_task(rotate_check())

        try:
            while not stop_event.is_set():

                if rotation_task.done():
                    break

                try:
                    raw_message = await asyncio.wait_for(
                        ws.recv(),
                        timeout=1.0,
                    )
                except asyncio.TimeoutError:
                    continue

                rx = time.time()

                try:
                    parsed = json.loads(raw_message)
                except json.JSONDecodeError:
                    log("[CLOB WARNING] Invalid JSON message")
                    continue

                for event in normalize_clob_message(parsed):
                    with stats_lock:
                        statistics["clob_events"] += 1
                        event_count = statistics["clob_events"]

                    # Store the raw event unchanged for replay.
                    save_record({
                        "rx": rx,
                        "ev": event,
                    })

                    if event_count == 1 or event_count % 100 == 0:
                        event_type = event.get("event_type", "unknown")
                        log(
                            f"[CLOB DATA] event={event_count} "
                            f"type={event_type}"
                        )

        finally:
            rotation_task.cancel()
            try:
                await rotation_task
            except asyncio.CancelledError:
                pass

    log("[CLOB] Market session closed")


async def clob_loop():
    """
    Discover the current market and rotate to the next 5-minute
    market. Discovery failures are retried without terminating
    the recorder.
    """
    while not stop_event.is_set():

        try:
            market = await asyncio.to_thread(discover_market)

            previous = get_current_market()
            if not previous or previous.get("slug") != market.get("slug"):
                set_current_market(market)
                record_market_metadata(market)

                log(
                    f"[MARKET DISCOVERED] {market['slug']} "
                    f"ends={market['market_end']}"
                )

            await clob_market_session(market)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log(f"[CLOB ERROR] {type(exc).__name__}: {exc}")

        if not stop_event.is_set():
            await asyncio.sleep(MARKET_REFRESH_SECONDS)

    log("[CLOB] Stopped")


# ============================================================
# SHUTDOWN
# ============================================================

def request_shutdown(signum=None, frame=None):
    if not stop_event.is_set():
        log(f"[SHUTDOWN] Signal received: {signum}")
        stop_event.set()


async def main_async():
    tasks = [
        asyncio.create_task(coinbase_loop(), name="coinbase"),
        asyncio.create_task(clob_loop(), name="polymarket-clob"),
    ]

    try:
        while not stop_event.is_set():
            await asyncio.sleep(0.5)

    finally:
        stop_event.set()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    writer_thread = threading.Thread(
        target=writer_loop,
        name="jsonl-writer",
        daemon=False,
    )
    writer_thread.start()

    log("=" * 60)
    log("POLYMARKET BTC UP/DOWN 5M - PAPER RECORDER")
    log("CURRENT MARKET ONLY | RAW DATA RECORDING")
    log("NO ORDERS - DATA COLLECTION ONLY")
    log(f"DATA_DIR={DATA_DIR}")
    log("=" * 60)

    try:
        asyncio.run(main_async())

    except KeyboardInterrupt:
        request_shutdown()

    finally:
        stop_event.set()

        # Allow the writer to persist every queued record.
        writer_thread.join(timeout=30)

        if writer_thread.is_alive():
            log(
                "[SHUTDOWN WARNING] Writer has not finished "
                "within 30 seconds"
            )

        with stats_lock:
            snapshot = dict(statistics)

        log(
            "[FINAL STATS] "
            f"btc_ticks={snapshot['btc_ticks']} "
            f"clob_events={snapshot['clob_events']} "
            f"records_written={snapshot['records_written']} "
            f"records_dropped={snapshot['records_dropped']} "
            f"writer_errors={snapshot['writer_errors']} "
            f"queue_remaining={record_queue.qsize()}"
        )

        log("[SHUTDOWN] Recorder stopped")


if __name__ == "__main__":
    if TRADING_ENABLED:
        raise RuntimeError(
            "Safety check failed: this program must not place orders."
        )

    main()
