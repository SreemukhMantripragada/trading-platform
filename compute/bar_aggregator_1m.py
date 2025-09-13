'''
bar_aggregator_1m.py

Aggregates 1-second bar data from a Kafka topic into 1-minute bars, writes the results to a PostgreSQL database,
and republishes the aggregated 1-minute bars to another Kafka topic. Also exposes Prometheus metrics for monitoring.

Main Features:
- Consumes 1s bar data from a Kafka topic (`bars.1s`).
- Aggregates incoming bars into 1-minute bars per symbol.
- Periodically flushes completed 1-minute bars to PostgreSQL (`bars_1m` table) using upsert logic.
- Publishes the aggregated 1-minute bars to an output Kafka topic (`bars.1s`).
- Exposes Prometheus metrics for flush durations, bars written, and bars published.
- Handles graceful shutdown and ensures all data is flushed on exit.

Configuration:
- Kafka broker, topics, consumer group, and PostgreSQL connection parameters are configurable via environment variables.
- Metrics server port and flush grace period are also configurable.

Classes:
- MBar: Represents an in-memory 1-minute bar and provides methods to merge 1s bars.

Functions:
- minute_start(sec): Returns the start timestamp of the minute for a given second.
- ensure_schema(pool): Ensures the target PostgreSQL table exists.
- main(): Main async entry point for the aggregator, sets up consumers, producers, state, and periodic flushing.

Usage:
    python bar_aggregator_1m.py

Environment Variables:
    KAFKA_BROKER, IN_TOPIC, OUT_TOPIC, KAFKA_GROUP,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
    METRICS_PORT
'''
import asyncio, os, time, ujson as json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
from prometheus_client import start_http_server, Counter, Histogram

# ---- Config (env) ----
BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("IN_TOPIC", "bars.1s")     # input: 1s bars stream
OUT_TOPIC = os.getenv("OUT_TOPIC", "bars.1m")    # output: 1m bars stream
GROUP_ID  = os.getenv("KAFKA_GROUP", "bars_1m_agg")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8004"))
FLUSH_GRACE_SEC = 5  # flush minute if (now - min_start) > 65s or on forced roll

# ---- SQL ----
DDL = """
CREATE TABLE IF NOT EXISTS bars_1m(
  symbol   text                     NOT NULL,
  ts       timestamptz              NOT NULL,
  o        double precision,
  h        double precision,
  l        double precision,
  c        double precision,
  vol      bigint,
  n_trades int,
  PRIMARY KEY(symbol, ts)
);
"""
UPSERT = """
INSERT INTO bars_1m(symbol, ts, o, h, l, c, vol, n_trades)
VALUES($1, to_timestamp($2), $3, $4, $5, $6, $7, $8)
ON CONFLICT(symbol, ts) DO UPDATE SET
  o = EXCLUDED.o,
  h = GREATEST(bars_1m.h, EXCLUDED.h),
  l = LEAST(bars_1m.l, EXCLUDED.l),
  c = EXCLUDED.c,
  vol = bars_1m.vol + EXCLUDED.vol,
  n_trades = bars_1m.n_trades + EXCLUDED.n_trades;
"""

# ---- Metrics ----
BATCH_FLUSH_SEC = Histogram("bars_1m_flush_seconds", "1m flush time", buckets=(0.005,0.01,0.02,0.05,0.1,0.2,0.5,1.0))
BARS_WRITTEN    = Counter("bars_1m_written_total", "1m bars written")
BARS_PUBLISHED  = Counter("bars_1m_published_total", "1m bars published")

class MBar:
    __slots__=("start","o","h","l","c","vol","n")
    def __init__(self, start:int, o:float, h:float, l:float, c:float, vol:int, n:int):
        self.start=start; self.o=o; self.h=h; self.l=l; self.c=c; self.vol=vol; self.n=n
    def merge_1s(self, o,h,l,c,vol,n):
        if self.n==0: self.o=o
        self.c=c; self.h=max(self.h,h); self.l=min(self.l,l); self.vol+=vol; self.n+=n

def minute_start(sec:int)->int: return sec - (sec % 60)

async def ensure_schema(pool):
    async with pool.acquire() as con: await con.execute(DDL)

async def main():
    start_http_server(METRICS_PORT)
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    await ensure_schema(pool)

    consumer = AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
        auto_offset_reset="earliest", group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()

    # state: dict[(symbol, minute_start)] = MBar
    state = {}

    async def flush_old(force_all=False, sym_min_keys=None):
        now_s = int(time.time())
        keys = list(state.keys()) if sym_min_keys is None else list(sym_min_keys)
        to_flush=[]
        for k in keys:
            sym, mstart = k
            # flush when minute is safely past (grace) or forced
            if force_all or (now_s >= mstart + 60 + FLUSH_GRACE_SEC):
                mb = state.pop(k, None)
                if mb: to_flush.append((sym, mb))
        if not to_flush: return
        @BATCH_FLUSH_SEC.time()
        async def _do(batch):
            async with pool.acquire() as con:
                async with con.transaction():
                    await con.executemany(
                        UPSERT,
                        [(sym, mb.start, mb.o, mb.h, mb.l, mb.c, mb.vol, mb.n) for sym, mb in batch]
                    )
            BARS_WRITTEN.inc(len(batch))
            for sym, mb in batch:
                payload = {"symbol": sym, "tf": "1m", "ts": mb.start,
                           "o": mb.o, "h": mb.h, "l": mb.l, "c": mb.c,
                           "vol": mb.vol, "n_trades": mb.n}
                await producer.send_and_wait(OUT_TOPIC, json.dumps(payload).encode(), key=sym.encode())
            BARS_PUBLISHED.inc(len(batch))
        await _do(to_flush)

    async def periodic_flush():
        try:
            while True:
                await asyncio.sleep(1.0)
                await flush_old()
        except asyncio.CancelledError:
            return

    flusher = asyncio.create_task(periodic_flush())

    try:
        while True:
            batches = await consumer.getmany(timeout_ms=1000, max_records=2000)
            for _, msgs in batches.items():
                for m in msgs:
                    r = m.value  # {"symbol","tf":"1s","ts":sec,"o","h","l","c","vol","n_trades"}
                    sym = r["symbol"]; sec = int(r["ts"]); ms = minute_start(sec)
                    k = (sym, ms)
                    mb = state.get(k)
                    if mb is None:
                        state[k] = MBar(ms, float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"]), int(r["vol"]), int(r["n_trades"]))
                    else:
                        mb.merge_1s(float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"]), int(r["vol"]), int(r["n_trades"]))
                    # opportunistic: if we see any key strictly older than current minute, try to flush it
                    # (periodic task also handles it)
            await consumer.commit()
    finally:
        await flush_old(force_all=True)
        flusher.cancel()
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__ == "__main__":
    asyncio.run(main())