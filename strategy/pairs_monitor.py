"""
Consumes bars.1m, maintains rolling z-score of spread for selected pairs, emits orders (ENTER/EXIT) via Kafka 'orders'.
Config knobs can be added later; default: enter if |z|>2, exit if |z|<0.5.
"""
import os, asyncio, ujson as json
from collections import deque, defaultdict
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg, numpy as np

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","bars.1m")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders")
GROUP_ID=os.getenv("KAFKA_GROUP","pairs_monitor")
PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

PAIRS=os.getenv("PAIRS","RELIANCE:INFY,TCS:WIPRO").split(",")  # A:B
WIN=int(os.getenv("PAIR_WIN","120"))  # 120 mins ~ 2h rolling

def mk_coid(symA,symB,ts): return f"PAIR:{symA}:{symB}:{ts}"

async def persist(pool, order): 
    async with pool.acquire() as con:
        await con.execute(
          "INSERT INTO orders(client_order_id, ts, symbol, side, qty, order_type, strategy, reason, risk_bucket, status, extra) "
          "VALUES($1,to_timestamp($2),$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING",
          order["client_order_id"], order["ts"], order["symbol"], order["side"], order["qty"], order["order_type"],
          order["strategy"], order["reason"], order["risk_bucket"], order["status"], json.dumps(order.get("extra",{}))
        )

async def main():
    pairs=[p.split(":") for p in PAIRS if ":" in p]
    buf=defaultdict(lambda: deque(maxlen=WIN))
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    cons=AIOKafkaConsumer(IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
                          auto_offset_reset="earliest", group_id=GROUP_ID,
                          value_deserializer=lambda b: json.loads(b.decode()),
                          key_deserializer=lambda b: b.decode() if b else None)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await cons.start(); await prod.start()
    try:
        while True:
            batches=await cons.getmany(timeout_ms=1000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    r=m.value; sym=r["symbol"]; ts=int(r["ts"]); c=float(r["c"])
                    buf[sym].append(c)

                    for A,B in pairs:
                        if sym not in (A,B): continue
                        if len(buf[A])<WIN or len(buf[B])<WIN: continue
                        sA=np.array(buf[A]); sB=np.array(buf[B])
                        spread=sA - sB; z=(spread[-1]-spread.mean())/(spread.std()+1e-9)
                        if abs(z)>2.0:
                            side = "LONG_A_SHORT_B" if z<0 else "SHORT_A_LONG_B"
                            for S,Sside in ((A, "BUY" if side=="LONG_A_SHORT_B" else "SELL"),
                                            (B, "SELL" if side=="LONG_A_SHORT_B" else "BUY")):
                                order={"client_order_id": mk_coid(A,B,ts)+":"+Sside,
                                       "ts": ts, "symbol": S, "side": Sside, "qty": 1, "order_type":"MKT",
                                       "strategy":"Pairs", "reason": f"z={z:.2f}", "risk_bucket":"LOW", "status":"NEW",
                                       "extra":{"pair":{"A":A,"B":B,"z":float(z),"win":WIN}}}
                                await persist(pool, order)
                                await prod.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=S.encode())
                        elif abs(z)<0.5:
                            for S in (A,B):
                                order={"client_order_id": mk_coid(A,B,ts)+":EXIT:"+S, "ts":ts, "symbol":S,
                                       "side":"EXIT","qty":0,"order_type":"MKT","strategy":"Pairs",
                                       "reason": f"z={z:.2f} exit", "risk_bucket":"LOW","status":"NEW","extra":{"pair_exit":True}}
                                await persist(pool, order)
                                await prod.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=S.encode())
            await cons.commit()
    finally:
        await cons.stop(); await prod.stop(); await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
