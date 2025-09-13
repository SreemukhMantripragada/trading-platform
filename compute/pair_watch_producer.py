"""
compute/pair_watch_producer.py
- Loads active pairs + beta from DB (pairs_universe.active=true)
- Consumes bars.1m for both legs
- Keeps rolling window of spread S = log(Pa) - beta*log(Pb)
- Emits ENTER/EXIT signals to Kafka topic (pairs.signals) when z-score crosses thresholds.

ENV:
  KAFKA_BROKER=localhost:9092
  IN_TOPIC=bars.1m
  OUT_TOPIC=pairs.signals
  WINDOW_MIN=120
  Z_ENTER=2.0
  Z_EXIT=0.5
  RELOAD_PAIRS_SEC=300

Run:
  python compute/pair_watch_producer.py
"""
from __future__ import annotations
import os, math, asyncio, asyncpg, ujson as json
from collections import deque, defaultdict
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","bars.1m")
OUT_TOPIC=os.getenv("OUT_TOPIC","pairs.signals")
WINDOW=int(os.getenv("WINDOW_MIN","120"))
Z_ENTER=float(os.getenv("Z_ENTER","2.0"))
Z_EXIT=float(os.getenv("Z_EXIT","0.5"))
RELOAD=int(os.getenv("RELOAD_PAIRS_SEC","300"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

async def load_pairs(pool):
    async with pool.acquire() as con:
        rows=await con.fetch("SELECT pair_id,a_symbol,b_symbol,beta FROM pairs_universe WHERE active=true")
    pairs=[(int(r["pair_id"]), r["a_symbol"], r["b_symbol"], float(r["beta"])) for r in rows]
    return pairs

def zscore(values: deque[float]) -> float|None:
    n=len(values)
    if n<30: return None
    m=sum(values)/n
    var=sum((x-m)**2 for x in values)/n
    sd=math.sqrt(max(var,1e-18))
    return (values[-1]-m)/sd if sd>0 else None

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    pairs=await load_pairs(pool); print(f"[pairwatch] loaded {len(pairs)} pairs")
    sym_to_pairs=defaultdict(list)  # symbol -> list of (pair_id, role, other_symbol, beta)
    for pid,a,b,beta in pairs:
        sym_to_pairs[a].append((pid,"A",b,beta))
        sym_to_pairs[b].append((pid,"B",a,beta))

    # reload task
    async def reloader():
        nonlocal pairs, sym_to_pairs
        while True:
            await asyncio.sleep(RELOAD)
            try:
                new=await load_pairs(pool)
                if new!=pairs:
                    pairs=new; sym_to_pairs=defaultdict(list)
                    for pid,a,b,beta in pairs:
                        sym_to_pairs[a].append((pid,"A",b,beta))
                        sym_to_pairs[b].append((pid,"B",a,beta))
                    print(f"[pairwatch] reloaded {len(pairs)} pairs")
            except Exception as e:
                print(f"[pairwatch] reload error: {e}")
    asyncio.create_task(reloader())

    # state
    last_px={}                          # symbol -> last price
    spreads=defaultdict(lambda: deque(maxlen=WINDOW))  # pair_id -> rolling spread window
    in_pos=set()                        # active pair positions (pair_id, dir), dir= +1 (LONG A / SHORT B) or -1

    consumer = AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False, auto_offset_reset="earliest",
        group_id="pair_watch_producer",
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    print(f"[pairwatch] IN={IN_TOPIC} → OUT={OUT_TOPIC}")

    try:
        while True:
            batches=await consumer.getmany(timeout_ms=500, max_records=2000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    r=m.value; sym=r["symbol"]; c=float(r["c"]); ts=int(r["ts"])
                    if sym not in sym_to_pairs: continue
                    last_px[sym]=c
                    for pid, role, other, beta in sym_to_pairs[sym]:
                        if other not in last_px: continue
                        pa = last_px[ sym ] if role=="A" else last_px[ other ]
                        pb = last_px[ other ] if role=="A" else last_px[ sym ]
                        if pa<=0 or pb<=0: continue
                        S = math.log(pa) - beta*math.log(pb)
                        spreads[pid].append(S)
                        z = zscore(spreads[pid])
                        if z is None: continue
                        # decide
                        action=None; sideA=sideB=None; direction=None
                        if abs(z) >= Z_ENTER:
                            if z>0 and (pid,-1) not in in_pos and (pid,1) not in in_pos:
                                # A rich vs B -> SELL A, BUY B
                                action="ENTER_SHORT_A_LONG_B"; sideA="SELL"; sideB="BUY"; direction=-1
                                in_pos.add((pid, direction))
                            elif z<0 and (pid,-1) not in in_pos and (pid,1) not in in_pos:
                                # A cheap -> BUY A, SELL B
                                action="ENTER_LONG_A_SHORT_B"; sideA="BUY"; sideB="SELL"; direction=+1
                                in_pos.add((pid, direction))
                        elif abs(z) <= Z_EXIT:
                            # if in pos, flatten
                            if (pid,1) in in_pos:
                                action="EXIT_LONG_A_SHORT_B"; sideA="SELL"; sideB="BUY"; in_pos.remove((pid,1)); direction=0
                            elif (pid,-1) in in_pos:
                                action="EXIT_SHORT_A_LONG_B"; sideA="BUY"; sideB="SELL"; in_pos.remove((pid,-1)); direction=0
                        if action:
                            msg={"ts": ts, "pair_id": pid, "a_symbol": (sym if role=='A' else other),
                                 "b_symbol": (other if role=='A' else sym), "beta": beta, "z": z,
                                 "action": action, "sideA": sideA, "sideB": sideB}
                            await producer.send_and_wait(OUT_TOPIC, json.dumps(msg).encode(), key=str(pid).encode())
            await consumer.commit()
    finally:
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
