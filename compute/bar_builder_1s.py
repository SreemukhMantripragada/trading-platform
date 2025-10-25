import asyncio, os, time, ujson as json
from collections import defaultdict
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
from prometheus_client import start_http_server, Counter, Histogram, Gauge

BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("IN_TOPIC", "ticks")
OUT_TOPIC = os.getenv("OUT_TOPIC", "bars.1s")
GROUP_ID  = os.getenv("KAFKA_GROUP", "bars_1s_builder")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8112"))
FLUSH_GRACE_SEC = 2

DDL = """
CREATE TABLE IF NOT EXISTS bars_1s(
  symbol   text                     NOT NULL,
  ts       timestamptz              NOT NULL,
  o        double precision,
  h        double precision,
  l        double precision,
  c        double precision,
  vol      bigint,
  n_trades int,
  PRIMARY KEY(symbol, ts)
);"""
UPSERT = """
INSERT INTO bars_1s(symbol, ts, o, h, l, c, vol, n_trades)
VALUES($1, to_timestamp($2), $3, $4, $5, $6, $7, $8)
ON CONFLICT(symbol, ts) DO UPDATE
SET o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c,
    vol=bars_1s.vol + EXCLUDED.vol, n_trades=bars_1s.n_trades + EXCLUDED.n_trades;"""

BARS_WRITTEN   = Counter("bars_1s_written_total", "1s bars written")
BARS_PUBLISHED = Counter("bars_1s_published_total", "1s bars published")
FLUSH_TIME     = Histogram("bars_1s_flush_seconds", "Flush batch seconds",
                           buckets=(0.005,0.01,0.02,0.05,0.1,0.2,0.5,1.0))
TICKS_INGESTED = Counter("bars_1s_ticks_total", "Ticks ingested by the 1s builder")
TICK_LATENCY   = Histogram(
    "bars_1s_tick_latency_seconds",
    "Latency between tick event timestamp and ingestion",
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0),
)
BAR_LATENCY    = Histogram(
    "bars_1s_publish_latency_seconds",
    "Latency between bar close and publish",
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)
OPEN_BARS      = Gauge("bars_1s_open_symbols", "Number of symbols with open 1s bars")

class Bar:
    __slots__ = ("o","h","l","c","vol","n_trades","sec")
    def __init__(self, px: float, sec: int, vol: int = 0):
        px=float(px); self.o=self.h=self.l=self.c=px
        self.vol=int(vol); self.n_trades=1; self.sec=int(sec)
    def update(self, px: float, vol: int = 0):
        px=float(px); self.c=px; self.h=max(self.h,px); self.l=min(self.l,px)
        self.vol += int(vol); self.n_trades += 1

async def ensure_schema(pool):
    async with pool.acquire() as con:   con.execute(DDL)

async def main():
    start_http_server(METRICS_PORT)
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS, min_size=1, max_size=4)
    await ensure_schema(pool)

    consumer = AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
        auto_offset_reset="earliest", group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None)
    producer=None
    if OUT_TOPIC:
        producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
        await producer.start()
    await consumer.start()

    bars = {}
    last_sec = defaultdict(lambda: None)
    OPEN_BARS.set(0)

    async def flush(symbols=None, force_old=False):
        now_s = int(time.time())
        sym_list = list(bars.keys()) if symbols is None else list(symbols)
        @FLUSH_TIME.time()
        async def _do(sym_list_inner):
            to_flush=[]
            for sym in sym_list_inner:
                b = bars.get(sym)
                if not b: continue
                if force_old or b.sec <= (now_s - FLUSH_GRACE_SEC):
                    to_flush.append((sym,b))
            if not to_flush:
                OPEN_BARS.set(len(bars))
                return
            async with pool.acquire() as con:
                async with con.transaction():
                    await con.executemany(UPSERT, [(s,br.sec,br.o,br.h,br.l,br.c,br.vol,br.n_trades) for s,br in to_flush])
            BARS_WRITTEN.inc(len(to_flush))
            if producer:
                for s,br in to_flush:
                    BAR_LATENCY.observe(max(0.0, time.time() - br.sec))
                    payload={"symbol":s,"tf":"1s","ts":br.sec,"o":br.o,"h":br.h,"l":br.l,"c":br.c,"vol":br.vol,"n_trades":br.n_trades}
                    await producer.send_and_wait(OUT_TOPIC, json.dumps(payload).encode(), key=s.encode())
                BARS_PUBLISHED.inc(len(to_flush))
            for s,_ in to_flush:
                bars.pop(s, None)
            OPEN_BARS.set(len(bars))
        await _do(sym_list)

    async def periodic_flush():
        try:
            while True:
                await asyncio.sleep(0.5)
                await flush()
        except asyncio.CancelledError:
            return

    flusher = asyncio.create_task(periodic_flush())
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=2000)
            for _, msgs in batches.items():
                for m in msgs:
                    r = m.value
                    sym = r["symbol"]
                    event_ns = int(r.get("event_ts") or 0)
                    if event_ns:
                        TICK_LATENCY.observe(max(0.0, (time.time_ns() - event_ns) / 1_000_000_000.0))
                        sec = int(event_ns / 1_000_000_000)
                    else:
                        sec = int(time.time())
                    px  = float(r["ltp"])
                    vol = int(r.get("vol") or 0)
                    TICKS_INGESTED.inc()
                    cur = bars.get(sym)
                    if cur is None:
                        bars[sym]=Bar(px,sec,vol); last_sec[sym]=sec
                    elif sec == cur.sec:
                        cur.update(px,vol)
                    else:
                        await flush([sym], force_old=True)
                        bars[sym]=Bar(px,sec,vol); last_sec[sym]=sec
                    OPEN_BARS.set(len(bars))
            await consumer.commit()
    finally:
        await flush(force_old=True)
        flusher.cancel()
        await consumer.stop()
        if producer: await producer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
