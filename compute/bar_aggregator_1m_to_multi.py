"""
1m -> multi-TF aggregator with EOS-style tx:
- In:  Kafka topic IN_TOPIC = 'bars.1m' (JSON {symbol,ts,o,h,l,c,vol,n_trades})
- Out: Kafka 'bars.3m','bars.5m','bars.15m','bars.60m' (same schema)
- DB:  UPSERT into tables bars_3m, bars_5m, bars_15m, bars_60m (symbol, ts PK)

Exactly-once-ish:
- Consumer commits via Kafka transactions (send_offsets_to_transaction)
- DB is idempotent (ON CONFLICT)
- Topics are compactable by key (symbol:ts)

ENV:
  KAFKA_BROKER=localhost:9092
  IN_TOPIC=bars.1m
  KAFKA_GROUP=agg_1m_multi
  TX_ID=agg-1m-to-multi
  OUT_TFS=3,5,15,60
  POSTGRES_HOST/PORT/DB/USER/PASSWORD
  METRICS_PORT=8005
"""
from __future__ import annotations
import os, time, asyncio, asyncpg, ujson as json
from collections import defaultdict
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition
from prometheus_client import start_http_server, Counter, Histogram
from libs.eos_tx import EOSTx

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","bars.1m")
GROUP_ID=os.getenv("KAFKA_GROUP","agg_1m_multi")
TX_ID=os.getenv("TX_ID","agg-1m-to-multi")
OUT_TFS=[int(x) for x in (os.getenv("OUT_TFS","3,5,15,60").split(","))]
METRICS_PORT=int(os.getenv("METRICS_PORT","8005"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

OUT_TOPICS={tf: f"bars.{tf}m" for tf in OUT_TFS}
TABLES   ={3:"bars_3m",5:"bars_5m",15:"bars_15m",60:"bars_60m"}

INS_SQL = {
 tf: f"""
INSERT INTO {TABLES[tf]}(symbol, ts, o, h, l, c, vol, n_trades)
VALUES($1, to_timestamp($2), $3,$4,$5,$6,$7,$8)
ON CONFLICT (symbol, ts) DO UPDATE
SET o=EXCLUDED.o, h=GREATEST({TABLES[tf]}.h, EXCLUDED.h),
    l=LEAST({TABLES[tf]}.l, EXCLUDED.l),
    c=EXCLUDED.c,
    vol={TABLES[tf]}.vol + EXCLUDED.vol,
    n_trades={TABLES[tf]}.n_trades + EXCLUDED.n_trades
""" for tf in OUT_TFS
}

BARS_OUT   = {tf: Counter(f"bars_{tf}m_out_total", f"{tf}m bars published") for tf in OUT_TFS}
FLUSH_TIME = Histogram("agg_flush_seconds", "Flush batch time", buckets=(0.002,0.005,0.01,0.02,0.05,0.1))
BATCH_SIZE = 2000
GRACE_SEC  = 3  # don't close current bucket too eagerly

class Bar:
    __slots__=("o","h","l","c","vol","n_trades","start")
    def __init__(self, px:float, start:int, vol:int, n:int):
        self.o=self.h=self.l=self.c=float(px)
        self.vol=int(vol); self.n_trades=int(n)
        self.start=int(start)
    def upd(self, px:float, vol:int, n:int):
        px=float(px)
        self.c=px; self.h=max(self.h,px); self.l=min(self.l,px)
        self.vol+=int(vol); self.n_trades+=int(n)

def bucket_start(ts:int, tf:int)->int:
    # ts: epoch seconds aligned to minute; tf in minutes
    return ts - (ts % (tf*60))

async def main():
    start_http_server(METRICS_PORT)
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)

    consumer = AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
        auto_offset_reset="earliest", group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=BROKER, acks="all", linger_ms=5,
        enable_idempotence=True, transactional_id=TX_ID
    )
    await consumer.start(); await producer.start()
    tx = EOSTx(producer); await tx.init()
    print(f"[agg] {IN_TOPIC} -> {','.join(OUT_TOPICS.values())} (txid={TX_ID})")

    # state: agg[tf][(sym, start)] -> Bar
    agg = {tf: {} for tf in OUT_TFS}

    async def flush_ready(force=False):
        now=int(time.time())
        to_flush = {tf: [] for tf in OUT_TFS}
        # select candidates
        for tf, m in agg.items():
            width = tf*60
            for (sym, st), bar in list(m.items()):
                if force or (st + width + GRACE_SEC) <= now:
                    to_flush[tf].append((sym, bar))
                    del m[(sym, st)]
        if all(len(v)==0 for v in to_flush.values()):
            return

        @FLUSH_TIME.time()
        async def _do():
            try:
                await tx.begin()
                async with pool.acquire() as con:
                    async with con.transaction():
                        # DB upserts
                        for tf, rows in to_flush.items():
                            if not rows: continue
                            await con.executemany(
                                INS_SQL[tf],
                                [(sym, b.start, b.o,b.h,b.l,b.c,b.vol,b.n_trades) for (sym,b) in rows]
                            )
                # Kafka publish
                for tf, rows in to_flush.items():
                    topic=OUT_TOPICS[tf]
                    for sym, b in rows:
                        key=f"{sym}:{b.start}".encode()
                        msg={"symbol":sym,"ts":b.start,"o":b.o,"h":b.h,"l":b.l,"c":b.c,"vol":b.vol,"n_trades":b.n_trades}
                        await producer.send(topic, json.dumps(msg).encode(), key=key)
                    BARS_OUT[tf].inc(len(rows))
                # offsets will be attached by caller (after getmany)
            except Exception:
                await tx.abort()
                raise
        await _do()

    async def periodic():
        while True:
            await asyncio.sleep(1.0)
            await flush_ready()
    flusher = asyncio.create_task(periodic())

    try:
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=BATCH_SIZE)
            if not batches:
                continue
            # build
            for tp, msgs in batches.items():
                for m in msgs:
                    r=m.value
                    sym=r["symbol"]; ts=int(r["ts"])
                    o,h,l,c,vol,n = float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"]),int(r["vol"]),int(r.get("n_trades",1))
                    for tf in OUT_TFS:
                        st=bucket_start(ts, tf)
                        k=(sym, st)
                        b=agg[tf].get(k)
                        if b is None:
                            agg[tf][k]=Bar(o, st, vol, n)
                        else:
                            b.upd(c, vol, n)

            # flush (may still keep hot buckets)
            await flush_ready()

            # transactional commit: bind offsets and commit
            # compute last offsets (+1) per tp
            offsets={}
            for tp, msgs in batches.items():
                offsets[tp]=msgs[-1].offset + 1
            await producer.send_offsets_to_transaction(offsets, GROUP_ID)
            await producer.commit_transaction()
    finally:
        flusher.cancel()
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
