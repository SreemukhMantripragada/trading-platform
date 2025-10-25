"""Aggregate 1m bars into higher timeframes and publish to Postgres and Kafka.

ENV:
  BAR_AGG_VERIFY           (default: 1) enable vendor verification loop
  BAR_AGG_VERIFY_INTERVAL_SEC (default: 300) interval between verification sweeps
  BAR_AGG_VERIFY_LOOKBACK  (default: 5) number of recent bars per symbol/tf to compare
  BAR_AGG_VERIFY_TOL_ABS   (default: 0.05) absolute OHLC tolerance when matching vendor data
  BAR_AGG_VERIFY_TOL_REL   (default: 0.001) relative OHLC tolerance (fraction of vendor value)
  BAR_AGG_VERIFY_TOL_VOL   (default: 5) volume tolerance in absolute lots
  KITE_API_KEY, KITE_TOKEN_JSON, ZERODHA_TOKENS_CSV required when verification enabled
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

import asyncpg
import ujson as json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server

try:  # Optional dependency for verification
    from kiteconnect import KiteConnect  # type: ignore
except ImportError:  # pragma: no cover - optional
    KiteConnect = None

# --- Environment -----------------------------------------------------------

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "bars.1m")
OUT_TFS = [int(x) for x in os.getenv("OUT_TFS", "3,5,15").split(",") if x.strip()]
if not OUT_TFS:
    OUT_TFS = [3, 5, 15]
OUT_TOPICS = {tf: os.getenv(f"OUT_TOPIC_{tf}", f"bars.{tf}m") for tf in OUT_TFS}

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

GROUP_ID = os.getenv("KAFKA_GROUP", "agg-1m-to-multi")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8113"))
BATCH_SIZE = int(os.getenv("BAR_AGG_BATCH_SIZE", "2000"))
GRACE_SEC = int(os.getenv("BAR_AGG_GRACE_SEC", "3"))

_FALSEY = {"0", "false", "no", "off"}
VERIFY_ENABLED = os.getenv("BAR_AGG_VERIFY", "1").lower() not in _FALSEY
VERIFY_INTERVAL_SEC = int(os.getenv("BAR_AGG_VERIFY_INTERVAL_SEC", "300"))
VERIFY_LOOKBACK = int(os.getenv("BAR_AGG_VERIFY_LOOKBACK", "5"))
VERIFY_TOL_ABS = float(os.getenv("BAR_AGG_VERIFY_TOL_ABS", "0.05"))
VERIFY_TOL_REL = float(os.getenv("BAR_AGG_VERIFY_TOL_REL", "0.001"))
VERIFY_TOL_VOL = int(os.getenv("BAR_AGG_VERIFY_TOL_VOL", "5"))
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_TOKEN_JSON = os.getenv("KITE_TOKEN_JSON", "ingestion/auth/token.json")
TOKENS_CSV = os.getenv("ZERODHA_TOKENS_CSV", "configs/tokens.csv")

# --- Metrics ---------------------------------------------------------------

BARS_IN = Counter("bar_agg_input_total", "Number of 1m bars ingested")
BARS_FLUSHED = {
    tf: Counter(f"bar_agg_output_{tf}m_total", f"Number of {tf}m bars flushed") for tf in OUT_TFS
}
FLUSH_SECONDS = Histogram(
    "bar_agg_flush_seconds",
    "Flush duration",
    buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0),
)
FLUSH_BATCH = {
    tf: Histogram(
        f"bar_agg_flush_batch_{tf}m_size",
        f"Number of bars flushed per batch for {tf}m",
        buckets=(1, 2, 5, 10, 20, 50, 100),
    )
    for tf in OUT_TFS
}
BAR_LATENCY = {
    tf: Histogram(
        f"bar_agg_bar_latency_{tf}m_seconds",
        f"Latency between bar close and publish for {tf}m",
        buckets=(0.5, 1, 2, 5, 10, 20, 40),
    )
    for tf in OUT_TFS
}
ACTIVE_WINDOWS = Gauge(
    "bar_agg_active_windows",
    "Number of in-flight aggregation windows per timeframe",
    ["tf"],
)
VERIFY_RUNS = Counter(
    "bar_agg_verify_runs_total",
    "Number of Zerodha verification sweeps executed",
    ["tf"],
)
VERIFY_MISMATCHES = Counter(
    "bar_agg_verify_mismatches_total",
    "Vendor vs local mismatch count",
    ["tf", "symbol", "reason"],
)
VERIFY_DURATION = Histogram(
    "bar_agg_verify_duration_seconds",
    "Duration of a verification sweep per timeframe",
    ["tf"],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 20, 40),
)
VERIFY_LAST_TS = Gauge(
    "bar_agg_verify_last_epoch",
    "Epoch seconds of the last verification run",
    ["tf"],
)

# --- SQL ------------------------------------------------------------------

INS_SQL = {
    tf: f"""
INSERT INTO bars_{tf}m (symbol, ts, o, h, l, c, vol, n_trades)
VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8)
ON CONFLICT (symbol, ts) DO UPDATE
SET h = GREATEST(excluded.h, bars_{tf}m.h),
    l = LEAST(excluded.l, bars_{tf}m.l),
    c = excluded.c,
    vol = bars_{tf}m.vol + excluded.vol,
    n_trades = bars_{tf}m.n_trades + excluded.n_trades;
"""
    for tf in OUT_TFS
}

VERIFY_UPSERT_SQL = {
    tf: f"""
INSERT INTO bars_{tf}m (symbol, ts, o, h, l, c, vol, n_trades)
VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, GREATEST($8, 1))
ON CONFLICT (symbol, ts) DO UPDATE
SET o = EXCLUDED.o,
    h = EXCLUDED.h,
    l = EXCLUDED.l,
    c = EXCLUDED.c,
    vol = EXCLUDED.vol,
    n_trades = GREATEST(bars_{tf}m.n_trades, EXCLUDED.n_trades);
"""
    for tf in OUT_TFS
}

INTERVAL_LABEL = {3: "3minute", 5: "5minute", 15: "15minute"}


def bucket_start(ts: int, tf_minutes: int) -> int:
    width = tf_minutes * 60
    return ts - (ts % width)


@dataclass(slots=True)
class Bar:
    o: float
    h: float
    l: float
    c: float
    vol: int
    n_trades: int
    start: int

    def update(self, o: float, h: float, l: float, c: float, vol: int, n_trades: int) -> None:
        self.c = c
        self.h = max(self.h, h)
        self.l = min(self.l, l)
        self.vol += vol
        self.n_trades += n_trades


class TimeframeAggregator:
    def __init__(self, tf_minutes: int):
        self.tf = tf_minutes
        self.width = tf_minutes * 60
        self.active: Dict[str, Bar] = {}

    def ingest(self, sym: str, ts: int, o: float, h: float, l: float, c: float, vol: int, n_trades: int) -> List[Tuple[str, Bar]]:
        ready: List[Tuple[str, Bar]] = []
        bucket = bucket_start(ts, self.tf)
        current = self.active.get(sym)
        if current and current.start != bucket:
            ready.append((sym, current))
            self.active.pop(sym, None)
            current = None
        if current is None:
            self.active[sym] = Bar(o=o, h=h, l=l, c=c, vol=vol, n_trades=n_trades, start=bucket)
        else:
            current.update(o, h, l, c, vol, n_trades)
        return ready

    def flush_expired(self, now: int) -> List[Tuple[str, Bar]]:
        ready: List[Tuple[str, Bar]] = []
        expire_before = now - GRACE_SEC
        for sym, bar in list(self.active.items()):
            if bar.start + self.width <= expire_before:
                ready.append((sym, bar))
                self.active.pop(sym, None)
        return ready

    def finalize_all(self) -> List[Tuple[str, Bar]]:
        ready = [(sym, bar) for sym, bar in self.active.items()]
        self.active.clear()
        return ready


def _load_access_token() -> Optional[str]:
    try:
        with open(KITE_TOKEN_JSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            token = data.get("access_token")
            if token and isinstance(token, str):
                return token
    except FileNotFoundError:
        print(f"[agg][verify] token file missing: {KITE_TOKEN_JSON}")
    except Exception as exc:
        print(f"[agg][verify] token load failed: {exc}")
    return None


def _load_symbol_tokens() -> Dict[str, int]:
    out: Dict[str, int] = {}
    try:
        with open(TOKENS_CSV, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                sym = (row.get("tradingsymbol") or "").strip().upper()
                tok = row.get("instrument_token")
                if not sym or not tok:
                    continue
                try:
                    out[sym] = int(tok)
                except ValueError:
                    continue
    except FileNotFoundError:
        print(f"[agg][verify] tokens CSV missing: {TOKENS_CSV}")
    except Exception as exc:
        print(f"[agg][verify] tokens CSV load failed: {exc}")
    return out


def _within(local: float, vendor: float) -> bool:
    return abs(local - vendor) <= max(VERIFY_TOL_ABS, abs(vendor) * VERIFY_TOL_REL)


async def _override_bar(
    pool: asyncpg.Pool,
    tf: int,
    symbol: str,
    epoch: int,
    vendor: Dict[str, float],
) -> None:
    sql = VERIFY_UPSERT_SQL.get(tf)
    if not sql:
        return
    await pool.execute(
        sql,
        symbol,
        epoch,
        vendor["o"],
        vendor["h"],
        vendor["l"],
        vendor["c"],
        int(vendor.get("vol", 0)),
        1,
    )


async def _verify_timeframe(
    pool: asyncpg.Pool,
    kite: "KiteConnect",
    token_map: Dict[str, int],
    tf: int,
    symbols: Set[str],
) -> None:
    tf_label = str(tf)
    interval = INTERVAL_LABEL.get(tf)
    if not interval:
        return
    start_time = time.perf_counter()
    now_utc = datetime.utcnow().replace(second=0, microsecond=0)
    start_utc = now_utc - timedelta(minutes=tf * (VERIFY_LOOKBACK + 2))
    end_utc = now_utc
    tolerance_vol = VERIFY_TOL_VOL
    mismatch_counter = 0

    for sym in sorted(symbols):
        token = token_map.get(sym.upper())
        if not token:
            continue

        try:
            vendor_rows = await asyncio.to_thread(
                kite.historical_data,
                token,
                start_utc,
                end_utc,
                interval,
                False,
                False,
            )
        except Exception as exc:
            print(f"[agg][verify] vendor fetch failed sym={sym} tf={tf}: {exc}")
            await asyncio.sleep(0.2)
            continue

        vendor_map: Dict[int, Dict[str, float]] = {}
        for row in vendor_rows:
            try:
                dt = row["date"].astimezone(timezone.utc).replace(second=0, microsecond=0)
                epoch = int(dt.timestamp())
                vendor_map[epoch] = {
                    "o": float(row.get("open", 0.0)),
                    "h": float(row.get("high", 0.0)),
                    "l": float(row.get("low", 0.0)),
                    "c": float(row.get("close", 0.0)),
                    "vol": int(row.get("volume", 0)),
                }
            except Exception:
                continue

        if not vendor_map:
            continue

        async with pool.acquire() as con:
            rows = await con.fetch(
                f"SELECT extract(epoch from ts)::bigint AS epoch, o, h, l, c, vol "
                f"FROM bars_{tf}m WHERE symbol=$1 AND ts BETWEEN to_timestamp($2) AND to_timestamp($3) "
                "ORDER BY ts",
                sym,
                int(start_utc.timestamp()),
                int(end_utc.timestamp()),
            )
        local_map: Dict[int, Dict[str, float]] = {
            int(r["epoch"]): {
                "o": float(r["o"]),
                "h": float(r["h"]),
                "l": float(r["l"]),
                "c": float(r["c"]),
                "vol": int(r["vol"] or 0),
            }
            for r in rows
        }

        keys = sorted(set(vendor_map.keys()) | set(local_map.keys()))
        if len(keys) > VERIFY_LOOKBACK:
            keys = keys[-VERIFY_LOOKBACK:]

        for epoch in keys:
            vendor = vendor_map.get(epoch)
            local = local_map.get(epoch)
            ts_display = datetime.fromtimestamp(epoch, timezone.utc)
            if vendor is None:
                continue
            if local is None:
                mismatch_counter += 1
                VERIFY_MISMATCHES.labels(tf=tf_label, symbol=sym, reason="missing_local").inc()
                print(f"[agg][verify] missing local bar sym={sym} tf={tf} ts={ts_display}; overriding with vendor")
                await _override_bar(pool, tf, sym, epoch, vendor)
                continue

            reasons = []
            for key in ("o", "h", "l", "c"):
                if not _within(local[key], vendor[key]):
                    reasons.append(key)
            if abs(local["vol"] - vendor["vol"]) > tolerance_vol:
                reasons.append("vol")

            if reasons:
                mismatch_counter += 1
                reason_str = ",".join(reasons)
                VERIFY_MISMATCHES.labels(tf=tf_label, symbol=sym, reason=reason_str).inc()
                print(
                    f"[agg][verify] mismatch sym={sym} tf={tf} ts={ts_display} reasons={reason_str};"
                    " applying vendor override"
                )
                await _override_bar(pool, tf, sym, epoch, vendor)

        await asyncio.sleep(0.1)

    VERIFY_RUNS.labels(tf=tf_label).inc()
    VERIFY_LAST_TS.labels(tf=tf_label).set(time.time())
    VERIFY_DURATION.labels(tf=tf_label).observe(time.perf_counter() - start_time)
    if mismatch_counter:
        print(f"[agg][verify] tf={tf} corrections applied count={mismatch_counter}")


async def verification_loop(pool: asyncpg.Pool, symbols_by_tf: Dict[int, Set[str]]) -> None:
    if not VERIFY_ENABLED:
        return
    if KiteConnect is None:
        print("[agg][verify] disabled: kiteconnect not installed")
        return
    if not KITE_API_KEY:
        print("[agg][verify] disabled: missing KITE_API_KEY")
        return
    token = _load_access_token()
    if not token:
        print("[agg][verify] disabled: missing Zerodha access token")
        return
    token_map = _load_symbol_tokens()
    if not token_map:
        print("[agg][verify] disabled: empty tokens map")
        return
    try:
        kite = KiteConnect(api_key=KITE_API_KEY)
        kite.set_access_token(token)
    except Exception as exc:
        print(f"[agg][verify] disabled: KiteConnect init failed ({exc})")
        return

    print(
        f"[agg][verify] enabled interval={VERIFY_INTERVAL_SEC}s lookback={VERIFY_LOOKBACK}"
    )

    try:
        while True:
            for tf in OUT_TFS:
                symbols = symbols_by_tf.get(tf, set())
                if symbols:
                    await _verify_timeframe(pool, kite, token_map, tf, symbols)
            await asyncio.sleep(VERIFY_INTERVAL_SEC)
    except asyncio.CancelledError:
        pass

async def flush(pool: asyncpg.Pool, producer: AIOKafkaProducer, ready: DefaultDict[int, List[Tuple[str, Bar]]]) -> int:
    total = sum(len(v) for v in ready.values())
    if total == 0:
        return 0
    start = time.perf_counter()
    async with pool.acquire() as con:
        async with con.transaction():
            for tf, items in ready.items():
                if not items:
                    continue
                rows = [(sym, bar.start, bar.o, bar.h, bar.l, bar.c, bar.vol, bar.n_trades) for sym, bar in items]
                await con.executemany(INS_SQL[tf], rows)
    for tf, items in ready.items():
        if not items:
            continue
        FLUSH_BATCH[tf].observe(len(items))
        topic = OUT_TOPICS[tf]
        width = tf * 60
        for sym, bar in items:
            payload = {
                "symbol": sym,
                "ts": bar.start,
                "o": bar.o,
                "h": bar.h,
                "l": bar.l,
                "c": bar.c,
                "vol": bar.vol,
                "n_trades": bar.n_trades,
            }
            bar_latency = max(0.0, time.time() - (bar.start + width))
            BAR_LATENCY[tf].observe(bar_latency)
            key = f"{sym}:{bar.start}".encode()
            await producer.send_and_wait(topic, json.dumps(payload).encode(), key=key)
        BARS_FLUSHED[tf].inc(len(items))
    FLUSH_SECONDS.observe(time.perf_counter() - start)
    return total


async def main() -> None:
    start_http_server(METRICS_PORT)

    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)

    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=BROKER,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None,
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)

    aggs = {tf: TimeframeAggregator(tf) for tf in OUT_TFS}
    for tf in OUT_TFS:
        ACTIVE_WINDOWS.labels(tf=str(tf)).set(0)

    symbols_by_tf: Dict[int, Set[str]] = {tf: set() for tf in OUT_TFS}
    verify_task: Optional[asyncio.Task] = None
    if VERIFY_ENABLED:
        verify_task = asyncio.create_task(verification_loop(pool, symbols_by_tf))

    await consumer.start()
    await producer.start()
    print(f"[agg] {IN_TOPIC} -> {', '.join(OUT_TOPICS.values())}")

    try:
        while True:
            batches = await consumer.getmany(timeout_ms=750, max_records=BATCH_SIZE)
            ready: DefaultDict[int, List[Tuple[str, Bar]]] = defaultdict(list)
            total_msgs = 0

            for tp, msgs in batches.items():
                if not msgs:
                    continue
                total_msgs += len(msgs)
                BARS_IN.inc(len(msgs))
                for msg in msgs:
                    record = msg.value
                    try:
                        sym = record["symbol"]
                        ts = int(record["ts"])
                        o = float(record["o"])
                        h = float(record["h"])
                        l = float(record["l"])
                        c = float(record["c"])
                        vol = int(record.get("vol", 0))
                        n_trades = int(record.get("n_trades", 1))
                    except (KeyError, TypeError, ValueError):
                        continue
                    for tf, agg in aggs.items():
                        symbols_by_tf.setdefault(tf, set()).add(sym)
                        finished = agg.ingest(sym, ts, o, h, l, c, vol, n_trades)
                        if finished:
                            ready[tf].extend(finished)

            now = int(time.time())
            for tf, agg in aggs.items():
                expired = agg.flush_expired(now)
                if expired:
                    ready[tf].extend(expired)

            flushed = await flush(pool, producer, ready)

            for tf, agg in aggs.items():
                ACTIVE_WINDOWS.labels(tf=str(tf)).set(len(agg.active))

            if flushed > 0:
                offsets: Dict[TopicPartition, int] = {}
                for tp, msgs in batches.items():
                    if msgs:
                        offsets[tp] = msgs[-1].offset + 1
                if offsets:
                    await consumer.commit(offsets=offsets)
            elif total_msgs == 0:
                await asyncio.sleep(0.2)

    finally:
        try:
            pending: DefaultDict[int, List[Tuple[str, Bar]]] = defaultdict(list)
            for tf, agg in aggs.items():
                remaining = agg.finalize_all()
                if remaining:
                    pending[tf].extend(remaining)
            await flush(pool, producer, pending)
        except Exception as exc:  # pragma: no cover - cleanup best effort
            print(f"[agg] final flush failed: {exc}")
        if verify_task:
            verify_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await verify_task
        await consumer.stop()
        await producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
