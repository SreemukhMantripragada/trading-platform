# strategy/pairs_live.py
# Live pair-trading engine: trades spread between (sym2 - beta*sym1).
from __future__ import annotations
import os, asyncio, time, ujson as json
from datetime import datetime, timezone, timedelta
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from collections import deque, defaultdict

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","bars.1m")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders")
GROUP_ID=os.getenv("KAFKA_GROUP","pairs_live")
REFRESH_SEC=int(os.getenv("PAIRS_REFRESH_SEC","30"))  # reload pairs_live periodically
SCAN_WINDOW_FALLBACK=int(os.getenv("PAIRS_WINDOW_M_FALLBACK","120"))  # if DB missing
RISK_BUCKET=os.getenv("PAIRS_BUCKET","MED")
PER_TRADE_NOTIONAL=float(os.getenv("PAIRS_PER_TRADE_NOTIONAL","20000"))  # guard will cap if needed

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

SQL_LOAD = """
SELECT sym1, sym2, beta, entry_z, exit_z, stop_z, window_m
FROM pairs_live WHERE trade_date = CURRENT_DATE
"""

IST = timezone(timedelta(hours=5, minutes=30))
def ist_minutes(ts:int)->int:
    dt=datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
    return dt.hour*60 + dt.minute

class ZRoll:
    """Rolling mean/std of spread for z-score using fixed window."""
    def __init__(self, window:int):
        self.w = int(window)
        self.q = deque(maxlen=self.w)
        self.s1 = 0.0
        self.s2 = 0.0
    def push(self, x:float):
        if len(self.q)==self.w:
            old=self.q[0]
            self.s1 -= old
            self.s2 -= old*old
        self.q.append(x)
        self.s1 += x
        self.s2 += x*x
    def ready(self)->bool:
        return len(self.q) >= max(30, int(0.5*self.w))
    def z(self, x:float)->float:
        n=len(self.q)
        if n<2: return 0.0
        m=self.s1/n
        v=max((self.s2/n) - m*m, 1e-9)
        return (x - m) / (v**0.5)

async def loader(pool):
    """Yield current pairs config from DB."""
    async with pool.acquire() as con:
        rows=await con.fetch(SQL_LOAD)
    pairs=[]
    for r in rows:
        pairs.append({
            "sym1": r["sym1"], "sym2": r["sym2"], "beta": float(r["beta"]),
            "entry_z": float(r["entry_z"]), "exit_z": float(r["exit_z"]), "stop_z": float(r["stop_z"]),
            "window_m": int(r["window_m"] or SCAN_WINDOW_FALLBACK)
        })
    return pairs

def qty_for(price:float, notional:float)->int:
    return max(1, int(notional // max(price, 1e-6)))

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    cons=AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, group_id=GROUP_ID, enable_auto_commit=False, auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: (b.decode() if b else None)
    )
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await cons.start(); await prod.start()
    print(f"[pairs_live] consuming {IN_TOPIC} → orders on {OUT_TOPIC}")

    # live state
    pairs = await loader(pool)     # list of dicts
    last_reload = time.time()

    # per-symbol last close
    last_c = {}
    # per-pair rolling stat and position
    roll = {}  # key=(s1,s2) -> ZRoll
    pos  = defaultdict(int)  # key=(s1,s2) -> {-1,0,+1} short spread / flat / long spread

    try:
        async for m in cons:
            r=m.value
            sym=r["symbol"]; c=float(r["c"]); ts=int(r["ts"])
            last_c[sym]=c

            # periodic reload of today pairs
            if time.time() - last_reload > REFRESH_SEC:
                pairs = await loader(pool)
                last_reload = time.time()

            # EOD flatten here for safety (OMS exit engine will also do this)
            if ist_minutes(ts) >= (15*60 + 15):
                for key, p in list(pos.items()):
                    if p==0: continue
                    s1,s2=key
                    # Exit both legs at market
                    coid=f"PAIRFLAT-{int(time.time())}-{s1}-{s2}"
                    # emit both legs
                    q1=abs(qty_for(last_c.get(s1, c), PER_TRADE_NOTIONAL/2))
                    q2=abs(qty_for(last_c.get(s2, c), PER_TRADE_NOTIONAL/2))
                    # reverse of entry direction
                    side1 = "BUY" if p<0 else "SELL"   # if we were short spread (p=-1), we were short sym2 & long sym1 => now BUY sym2 SELL sym1; but simple flatten via two orders below deals with sign.
                    side2 = "SELL" if p<0 else "BUY"
                    # For clarity, perform close with correct sides:
                    side_s2 = "BUY" if p<0 else "SELL"
                    side_s1 = "SELL" if p<0 else "BUY"
                    o1={"client_order_id": f"{coid}-1", "symbol": s2, "side": side_s2, "qty": q2, "price": 0.0,
                        "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                        "extra":{"px_ref": float(last_c.get(s2,0.0)), "reason":"EOD_FLATTEN"}}
                    o2={"client_order_id": f"{coid}-2", "symbol": s1, "side": side_s1, "qty": q1, "price": 0.0,
                        "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                        "extra":{"px_ref": float(last_c.get(s1,0.0)), "reason":"EOD_FLATTEN"}}
                    await prod.send_and_wait(OUT_TOPIC, json.dumps(o1).encode(), key=s2.encode())
                    await prod.send_and_wait(OUT_TOPIC, json.dumps(o2).encode(), key=s1.encode())
                    pos[key]=0

            # evaluate pairs that include this symbol
            for cfg in pairs:
                s1, s2, beta = cfg["sym1"], cfg["sym2"], float(cfg["beta"])
                if sym not in (s1, s2): 
                    continue
                # need both prices known
                if s1 not in last_c or s2 not in last_c:
                    continue
                spread = last_c[s2] - beta * last_c[s1]
                key=(s1,s2)
                if key not in roll:
                    roll[key]=ZRoll(cfg["window_m"])
                zr=roll[key]
                zr.push(spread)
                if not zr.ready():
                    continue
                z = zr.z(spread)
                p = pos[key]  # -1 short spread, +1 long spread, 0 flat
                # Entry logic:
                # z > entry => short spread: SELL sym2, BUY sym1*beta
                # z < -entry => long spread: BUY sym2, SELL sym1*beta
                entry, exit_z, stop = cfg["entry_z"], cfg["exit_z"], cfg["stop_z"]

                q1 = qty_for(last_c[s1], PER_TRADE_NOTIONAL/2)
                q2 = qty_for(last_c[s2], PER_TRADE_NOTIONAL/2)

                if p==0 and z >= entry:
                    # short spread
                    coid=f"PAIR-{int(time.time())}-{s1}-{s2}-S"
                    o_leg2={"client_order_id": f"{coid}-a", "symbol": s2, "side": "SELL", "qty": q2, "price": 0.0,
                            "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                            "extra":{"px_ref": float(last_c[s2]), "reason": f"PAIR z={z:.2f}>=entry {entry}"}}
                    o_leg1={"client_order_id": f"{coid}-b", "symbol": s1, "side": "BUY",  "qty": q1, "price": 0.0,
                            "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                            "extra":{"px_ref": float(last_c[s1]), "reason": f"PAIR hedge beta~{beta:.2f}"}}
                    await prod.send_and_wait(OUT_TOPIC, json.dumps(o_leg2).encode(), key=s2.encode())
                    await prod.send_and_wait(OUT_TOPIC, json.dumps(o_leg1).encode(), key=s1.encode())
                    pos[key]=-1
                elif p==0 and z <= -entry:
                    # long spread
                    coid=f"PAIR-{int(time.time())}-{s1}-{s2}-L"
                    o_leg2={"client_order_id": f"{coid}-a", "symbol": s2, "side": "BUY",  "qty": q2, "price": 0.0,
                            "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                            "extra":{"px_ref": float(last_c[s2]), "reason": f"PAIR z={z:.2f}<=-entry {-entry}"}}
                    o_leg1={"client_order_id": f"{coid}-b", "symbol": s1, "side": "SELL", "qty": q1, "price": 0.0,
                            "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                            "extra":{"px_ref": float(last_c[s1]), "reason": f"PAIR hedge beta~{beta:.2f}"}}
                    await prod.send_and_wait(OUT_TOPIC, json.dumps(o_leg2).encode(), key=s2.encode())
                    await prod.send_and_wait(OUT_TOPIC, json.dumps(o_leg1).encode(), key=s1.encode())
                    pos[key]=+1
                # Exit logic: reversion to |z|<=exit_z or stop breach
                elif p!=0:
                    need_exit = (abs(z) <= exit_z) or (abs(z) >= stop)
                    if need_exit:
                        coid=f"PAIRX-{int(time.time())}-{s1}-{s2}"
                        # close both legs (reverse of entry)
                        if p<0:
                            # was short spread => short s2, long s1; exit => BUY s2, SELL s1
                            o1={"client_order_id": f"{coid}-1", "symbol": s2, "side":"BUY",  "qty": q2, "price":0.0,
                                "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                                "extra":{"px_ref": float(last_c[s2]), "reason": f"PAIR EXIT z={z:.2f}"}}
                            o2={"client_order_id": f"{coid}-2", "symbol": s1, "side":"SELL", "qty": q1, "price":0.0,
                                "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                                "extra":{"px_ref": float(last_c[s1]), "reason": f"PAIR EXIT z={z:.2f}"}}
                        else:
                            # was long spread => long s2, short s1; exit => SELL s2, BUY s1
                            o1={"client_order_id": f"{coid}-1", "symbol": s2, "side":"SELL", "qty": q2, "price":0.0,
                                "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                                "extra":{"px_ref": float(last_c[s2]), "reason": f"PAIR EXIT z={z:.2f}"}}
                            o2={"client_order_id": f"{coid}-2", "symbol": s1, "side":"BUY",  "qty": q1, "price":0.0,
                                "strategy":"PAIRS_LIVE", "risk_bucket":RISK_BUCKET, "created_at":int(time.time()),
                                "extra":{"px_ref": float(last_c[s1]), "reason": f"PAIR EXIT z={z:.2f}"}}
                        await prod.send_and_wait(OUT_TOPIC, json.dumps(o1).encode(), key=s2.encode())
                        await prod.send_and_wait(OUT_TOPIC, json.dumps(o2).encode(), key=s1.encode())
                        pos[key]=0

            await cons.commit()
    finally:
        await cons.stop(); await prod.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
