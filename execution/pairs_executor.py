"""
execution/pairs_executor.py
Turns pairs.signals into two-leg orders (enter/exit). Budget from risk_budget.runtime.yaml.

Signal message (pairs.signals):
{
  "pair_id":"REL_TCS", "a_symbol":"RELIANCE", "b_symbol":"TCS",
  "beta":0.8, "z":2.1, "action":"ENTER_LONG_A_SHORT_B",  // or ENTER_SHORT_A_LONG_B or EXIT
  "risk_bucket":"MED", "pxA":2901.5, "pxB":4102.0, "ts": 1736499999000
}

ENV:
  KAFKA_BROKER=localhost:9092
  IN_TOPIC=pairs.signals
  OUT_TOPIC=orders
  GROUP_ID=pairs_executor
  RISK_BUDGET=configs/risk_budget.runtime.yaml
  METRICS_PORT=8020
"""
from __future__ import annotations
import os, asyncio, ujson as json, time, yaml, asyncpg, math
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server, Counter
from execution.throttle import Throttles

BROKER   = os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC","pairs.signals")
OUT_TOPIC= os.getenv("OUT_TOPIC","orders")
GROUP_ID = os.getenv("GROUP_ID","pairs_executor")
BUDGET_YAML = os.getenv("RISK_BUDGET","configs/risk_budget.runtime.yaml")
METRICS_PORT= int(os.getenv("METRICS_PORT","8020"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

ORDERS_EMIT = Counter("pairs_orders_emitted_total","orders emitted",["pair_id","leg","bucket","action"])
SIGNALS_SEEN= Counter("pairs_signals_total","signals processed",["action"])
REJECTS     = Counter("pairs_rejects_total","signals rejected",["reason"])

def load_budget():
    # Expect:
    # buckets:
    #   MED: { split: 0.3, max_per_trade: 20000 }
    try:
        cfg=yaml.safe_load(open(BUDGET_YAML))
        return cfg.get("buckets",{})
    except Exception:
        return {"MED":{"split":0.3,"max_per_trade":20000}}

async def positions_today(pool, pair_id:str):
    """
    Reads today's net positions for the pair legs from fills.extra JSON.
    Requires fills.extra->>'pair_id' to be set by gateways.
    Returns dict { "A": net_qty, "B": net_qty }
    """
    async with pool.acquire() as con:
        rows = await con.fetch("""
          SELECT (extra->>'leg') AS leg,
                 SUM(CASE WHEN side='BUY' THEN qty ELSE -qty END)::int AS net
          FROM fills
          WHERE ts::date = CURRENT_DATE AND (extra->>'pair_id') = $1
          GROUP BY leg
        """, pair_id)
    out={"A":0,"B":0}
    for r in rows:
        if r["leg"] in ("A","B"):
            out[r["leg"]] = int(r["net"] or 0)
    return out

def size_legs(notional:float, pxA:float, pxB:float, beta:float):
    """
    Dollar-neutral sizing: weights 1 (A) and beta (B).
    qtyA ~ notional / (pxA + beta*pxB); qtyB ~ beta * qtyA
    """
    pxA=max(pxA,1e-6); pxB=max(pxB,1e-6); beta=max(beta,1e-6)
    denom = pxA + beta*pxB
    if denom <= 0: return 0,0
    qA = math.floor( (notional/denom) )
    qB = math.floor( beta * qA )
    return max(qA,0), max(qB,0)

def coid(pair_id:str, leg:str):
    return f"PAIR:{pair_id}:{leg}:{int(time.time()*1000)}"

async def main():
    start_http_server(METRICS_PORT)
    buckets = load_budget()
    # simple throttles (symbol & bucket level)
    T = Throttles(sym_qps=1.0, bucket_qps={"LOW":1.0,"MED":0.7,"HIGH":0.4})

    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    consumer=AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, group_id=GROUP_ID, enable_auto_commit=False, auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    print(f"[pairs-exec] {IN_TOPIC} → {OUT_TOPIC} using {BUDGET_YAML}")
    try:
        async for m in consumer:
            s=m.value; act=(s.get("action") or "").upper()
            pair_id=s["pair_id"]; a=s["a_symbol"]; b=s["b_symbol"]; beta=float(s.get("beta",1.0))
            bucket=(s.get("risk_bucket") or "MED").upper()
            pxA=float(s.get("pxA") or 0.0); pxB=float(s.get("pxB") or 0.0)
            max_per_trade=float(buckets.get(bucket,{}).get("max_per_trade",20000.0))
            SIGNALS_SEEN.labels(act).inc()

            # require prices to size
            if act.startswith("ENTER") and (pxA<=0 or pxB<=0):
                REJECTS.labels("no_px").inc(); await consumer.commit(); continue

            # exits use net positions
            if act=="EXIT":
                net = await positions_today(pool, pair_id)
                qA, qB = abs(net["A"]), abs(net["B"])
                if qA==0 and qB==0:
                    await consumer.commit(); continue
                # directions for exit = flatten to zero
                sideA = "SELL" if net["A"]>0 else "BUY"
                sideB = "SELL" if net["B"]>0 else "BUY"
                await T.allow(a, bucket); await T.allow(b, bucket)
                oA={"client_order_id":coid(pair_id,"A"), "symbol":a, "side":sideA, "qty":qA,
                    "order_type":"MKT", "strategy":"PAIRS", "risk_bucket":bucket,
                    "extra":{"pair_id":pair_id,"leg":"A","reason":"EXIT"}}
                oB={"client_order_id":coid(pair_id,"B"), "symbol":b, "side":sideB, "qty":qB,
                    "order_type":"MKT", "strategy":"PAIRS", "risk_bucket":bucket,
                    "extra":{"pair_id":pair_id,"leg":"B","reason":"EXIT"}}
                await producer.send_and_wait(OUT_TOPIC, json.dumps(oA).encode(), key=a.encode())
                await producer.send_and_wait(OUT_TOPIC, json.dumps(oB).encode(), key=b.encode())
                ORDERS_EMIT.labels(pair_id,"A",bucket,"EXIT").inc()
                ORDERS_EMIT.labels(pair_id,"B",bucket,"EXIT").inc()
                await consumer.commit(); continue

            # entries
            qA,qB = size_legs(max_per_trade, pxA, pxB, beta)
            if qA<1 or qB<1:
                REJECTS.labels("tiny_qty").inc(); await consumer.commit(); continue

            if act=="ENTER_LONG_A_SHORT_B":
                sideA, sideB = "BUY","SELL"
            elif act=="ENTER_SHORT_A_LONG_B":
                sideA, sideB = "SELL","BUY"
            else:
                REJECTS.labels("bad_action").inc(); await consumer.commit(); continue

            await T.allow(a, bucket); await T.allow(b, bucket)

            oA={"client_order_id":coid(pair_id,"A"), "symbol":a, "side":sideA, "qty":qA,
                "order_type":"MKT", "strategy":"PAIRS", "risk_bucket":bucket,
                "extra":{"pair_id":pair_id,"leg":"A","beta":beta,"px_ref":pxA}}
            oB={"client_order_id":coid(pair_id,"B"), "symbol":b, "side":sideB, "qty":qB,
                "order_type":"MKT", "strategy":"PAIRS", "risk_bucket":bucket,
                "extra":{"pair_id":pair_id,"leg":"B","beta":beta,"px_ref":pxB}}
            await producer.send_and_wait(OUT_TOPIC, json.dumps(oA).encode(), key=a.encode())
            await producer.send_and_wait(OUT_TOPIC, json.dumps(oB).encode(), key=b.encode())
            ORDERS_EMIT.labels(pair_id,"A",bucket,"ENTER").inc()
            ORDERS_EMIT.labels(pair_id,"B",bucket,"ENTER").inc()
            await consumer.commit()
    finally:
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
