import os, asyncio, asyncpg, ujson as json, time
from aiokafka import AIOKafkaProducer
from datetime import datetime, timezone

BROKER=os.getenv("KAFKA_BROKER","localhost:9092"); TOPIC=os.getenv("TOPIC","ticks")
PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

async def replay(symbol:str, start_utc:str, end_utc:str, speed:float=4.0):
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER,acks="all"); await prod.start()
    s=datetime.fromisoformat(start_utc.replace("Z","+00:00")); e=datetime.fromisoformat(end_utc.replace("Z","+00:00"))
    n=0
    try:
        async with pool.acquire() as con:
            rows=await con.fetch("SELECT ts,o,h,l,c,vol FROM bars_1m WHERE symbol=$1 AND ts BETWEEN $2 AND $3 ORDER BY ts", symbol, s, e)
        prev=None
        for r in rows:
            ts=r["ts"].astimezone(timezone.utc); delay=0.0 if prev is None else max(0.0, (ts-prev).total_seconds()/speed)
            await asyncio.sleep(delay); prev=ts
            # emit 4 synthetic ticks per minute bar
            for frac,px in enumerate([r["o"],r["h"],r["l"],r["c"]], start=1):
                evt_ns=int(time.time_ns())
                msg={"symbol":symbol,"event_ts":evt_ns,"ltp":float(px),"vol":max(1,int(r["vol"]//4))}
                await prod.send_and_wait(TOPIC, json.dumps(msg).encode(), key=symbol.encode()); n+=1
        print(f"[replay] emitted {n} ticks for {symbol}")
    finally:
        await prod.stop(); await pool.close()

if __name__=="__main__":
    import sys, asyncio
    if len(sys.argv)<4: raise SystemExit("usage: SYMBOL START_ISO END_ISO [speed]")
    asyncio.run(replay(sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4]) if len(sys.argv)>4 else 4.0))
