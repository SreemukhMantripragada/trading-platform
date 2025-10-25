# compute/bar_aggregator_from_1m.py
import os, asyncio, time, ujson as json, asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from collections import defaultdict

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","bars.1m")
OUT_TFS=[("bars.3m",3),("bars.5m",5),("bars.15m",15)]
GROUP_ID=os.getenv("KAFKA_GROUP","agg_from_1m")
PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

UPSERT_FMT="""
INSERT INTO {table}(symbol,ts,o,h,l,c,vol,n_trades)
VALUES($1,to_timestamp($2),$3,$4,$5,$6,$7,$8)
ON CONFLICT(symbol,ts) DO UPDATE SET
h=GREATEST({table}.h, EXCLUDED.h),
l=LEAST({table}.l, EXCLUDED.l),
c=EXCLUDED.c,
vol={table}.vol + EXCLUDED.vol,
n_trades={table}.n_trades + EXCLUDED.n_trades;
"""

def bucket_ts(ts_sec:int, m:int)->int:
    return (ts_sec // 60 // m) * 60 * m

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    cons=AIOKafkaConsumer(IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
                          auto_offset_reset="earliest", group_id=GROUP_ID,
                          value_deserializer=lambda b: json.loads(b.decode()),
                          key_deserializer=lambda b: b.decode() if b else None)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await cons.start(); await prod.start()
    upserts={tf: UPSERT_FMT.format(table=f"bars_{tf.split('.')[1]}") for tf,_ in OUT_TFS}
    try:
        while True:
            batches=await cons.getmany(timeout_ms=1000, max_records=2000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    r=m.value; sym=r["symbol"]; ts=int(r["ts"]); o,h,l,c=float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"]); v=int(r.get("vol") or 0); n=int(r.get("n_trades") or 1)
                    for tf, M in OUT_TFS:
                        bts=bucket_ts(ts, M)
                        async with pool.acquire() as con:
                            await con.execute(upserts[tf], sym, bts, o, h, l, c, v, n)
                        out = {"symbol":sym,"ts":bts,"o":o,"h":h,"l":l,"c":c,"vol":v,"n_trades":n,"tf":f"{M}m"}
                        await prod.send_and_wait(tf, json.dumps(out).encode(), key=sym.encode())
            await cons.commit()
    finally:
        await cons.stop(); await prod.stop(); await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
