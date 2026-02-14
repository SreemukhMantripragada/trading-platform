#!/usr/bin/env python3
"""Synthetic Zerodha WS simulator for manual pipeline/load testing.

This script mimics the output contract of ingestion/zerodha_ws.py:
Kafka topic messages shaped as {"symbol","event_ts","ltp","vol"}.
"""

from __future__ import annotations

import asyncio
import csv
import heapq
import os
import random
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

import ujson as json
from aiokafka import AIOKafkaProducer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("TICKS_TOPIC", "ticks")
TOKENS_CSV = os.getenv("ZERODHA_TOKENS_CSV", "configs/tokens.csv")
SIM_SYMBOLS = int(os.getenv("SIM_SYMBOLS", "200"))
SIM_STEP_MS = int(os.getenv("SIM_STEP_MS", os.getenv("SIM_INTERVAL_MS", "100")))
SIM_BASE_TPS = float(os.getenv("SIM_BASE_TPS", "10.0"))
SIM_HOT_TPS = float(os.getenv("SIM_HOT_TPS", "1000.0"))
SIM_HOT_SYMBOL_PCT = float(os.getenv("SIM_HOT_SYMBOL_PCT", "0.0"))
SIM_HOT_ROTATE_SEC = float(os.getenv("SIM_HOT_ROTATE_SEC", "15.0"))
SIM_MAX_INFLIGHT = int(os.getenv("SIM_MAX_INFLIGHT", "12000"))
SIM_BASE_PRICE = float(os.getenv("SIM_BASE_PRICE", "2500"))
SIM_VOL_MIN = int(os.getenv("SIM_VOL_MIN", "1"))
SIM_VOL_MAX = int(os.getenv("SIM_VOL_MAX", "5"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8111"))

QUEUE_DEPTH = Gauge("zerodha_queue_depth", "Number of ticks buffered for Kafka dispatch")
TICKS_RECEIVED = Counter("zerodha_ticks_received_total", "Ticks forwarded to Kafka", ["symbol"])
TICKS_DROPPED = Counter("zerodha_ticks_dropped_total", "Ticks dropped due to full queue")
BATCH_SIZE = Histogram(
    "zerodha_tick_batch_size",
    "Batch size of ticks forwarded to Kafka",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000),
)
TICK_LATENCY = Histogram(
    "zerodha_tick_latency_seconds",
    "Latency between event timestamp and Kafka publish",
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0),
)
WS_EVENTS = Counter("zerodha_ws_events_total", "Websocket lifecycle events", ["event"])


def _load_symbols() -> List[str]:
    path = Path(TOKENS_CSV)
    if path.exists():
        symbols: List[str] = []
        with path.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                sub = str(row.get("subscribe", "1")).strip().lower()
                if sub not in {"1", "true", "yes", "y"}:
                    continue
                sym = str(row.get("tradingsymbol", "")).strip().upper()
                if sym:
                    symbols.append(sym)
        if symbols:
            return symbols
    return [f"SIM{i:04d}" for i in range(1, SIM_SYMBOLS + 1)]


def _pick_hot_symbols(symbols: List[str], pct: float) -> Set[str]:
    if pct <= 0 or not symbols:
        return set()
    want = int(len(symbols) * pct)
    if want <= 0:
        want = 1
    if want >= len(symbols):
        return set(symbols)
    return set(random.sample(symbols, want))


def _symbol_rate(sym: str, hot_symbols: Set[str]) -> float:
    return SIM_HOT_TPS if sym in hot_symbols else SIM_BASE_TPS


def _next_gap_sec(rate_tps: float) -> float:
    if rate_tps <= 0:
        return 1.0
    # Poisson-like arrivals avoid synchronized symbol bursts.
    return max(0.0005, random.expovariate(rate_tps))


async def main() -> None:
    start_http_server(METRICS_PORT)
    WS_EVENTS.labels("connect").inc()
    symbols = _load_symbols()
    prices: Dict[str, float] = {s: SIM_BASE_PRICE + random.uniform(-5.0, 5.0) for s in symbols}
    max_sleep_s = max(0.01, SIM_STEP_MS / 1000.0)
    hot_symbols: Set[str] = _pick_hot_symbols(symbols, SIM_HOT_SYMBOL_PCT)
    next_hot_rotate = time.monotonic() + max(1.0, SIM_HOT_ROTATE_SEC)
    schedule: List[Tuple[float, str]] = []
    now = time.monotonic()
    for sym in symbols:
        spread = random.uniform(0.0, 1.0)
        heapq.heappush(schedule, (now + spread, sym))
    print(
        f"[ws-sim] symbols={len(symbols)} base_tps={SIM_BASE_TPS:.2f} "
        f"hot_tps={SIM_HOT_TPS:.2f} hot_pct={SIM_HOT_SYMBOL_PCT:.4f} "
        f"mode=live-stream max_sleep_ms={int(max_sleep_s * 1000)}",
        flush=True,
    )

    producer = AIOKafkaProducer(bootstrap_servers=BROKER, linger_ms=5, acks="all")
    await producer.start()
    pending = []
    try:
        while True:
            now = time.monotonic()
            if now >= next_hot_rotate:
                hot_symbols = _pick_hot_symbols(symbols, SIM_HOT_SYMBOL_PCT)
                next_hot_rotate = now + max(1.0, SIM_HOT_ROTATE_SEC)

            if pending:
                pending = [fut for fut in pending if not fut.done()]
            if len(pending) >= SIM_MAX_INFLIGHT:
                _, pending_set = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                pending = list(pending_set)

            batch_count = 0
            while schedule and schedule[0][0] <= now and batch_count < 2000:
                _, sym = heapq.heappop(schedule)
                drift = random.uniform(-0.4, 0.4)
                px = max(1.0, prices[sym] + drift)
                prices[sym] = px
                vol = random.randint(SIM_VOL_MIN, SIM_VOL_MAX)
                event_ts = time.time_ns()
                payload = {
                    "symbol": sym,
                    "event_ts": event_ts,
                    "ltp": round(px, 2),
                    "vol": vol,
                }
                try:
                    pending.append(
                        await producer.send(
                            TOPIC,
                            json.dumps(payload).encode("utf-8"),
                            key=sym.encode("utf-8"),
                        )
                    )
                except Exception:
                    TICKS_DROPPED.inc()
                else:
                    TICKS_RECEIVED.labels(sym).inc()
                    TICK_LATENCY.observe(max(0.0, (time.time_ns() - event_ts) / 1_000_000_000.0))
                    batch_count += 1
                next_gap = _next_gap_sec(_symbol_rate(sym, hot_symbols))
                heapq.heappush(schedule, (time.monotonic() + next_gap, sym))
                now = time.monotonic()

            if batch_count > 0:
                BATCH_SIZE.observe(batch_count)
            QUEUE_DEPTH.set(float(len(pending)))
            WS_EVENTS.labels("ticks").inc(batch_count)
            next_due = schedule[0][0] if schedule else (time.monotonic() + max_sleep_s)
            wake_at = min(next_due, next_hot_rotate)
            sleep_for = min(max_sleep_s, max(0.0, wake_at - time.monotonic()))
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    except asyncio.CancelledError:
        WS_EVENTS.labels("close").inc()
        raise
    finally:
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
