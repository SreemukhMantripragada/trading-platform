"""
strategy/pairs_meanrev.py
- Consumes pairs.signals from pair_watch_producer.py
- On ENTER_* emits two orders (A & B) sized to ~equal market value, hedged by beta
- On EXIT_* emits flattening orders for current quantities (tracks in-process pos)

ENV:
  KAFKA_BROKER=localhost:9092
  IN_TOPIC=pairs.signals
  OUT_TOPIC=orders
  RISK_BUDGET=configs/risk_budget.yaml
  PAIRS_CFG=configs/pairs.yaml
  LIVE_USAGE_RATIO=0.80  (optional, for live cash if token present)

Run:
  python strategy/pairs_meanrev.py
"""
from __future__ import annotations
import os, yaml, math, asyncio, asyncpg, ujson as json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv(".env"); load_dotenv("infra/.env")

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","pairs.signals")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders")
RISK_BUDGET_YML=os.getenv("RISK_BUDGET","configs/risk_budget.yaml")
PAIRS_CFG=os.getenv("PAIRS_CFG","configs/pairs.yaml")
LIVE_USAGE_RATIO=float(os.getenv("LIVE_USAGE_RATIO","0.80"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

KITE_API_KEY=os.getenv("KITE_API_KEY"); TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

def load_cfgs():
    rb=yaml.safe_load(open(RISK_BUDGET_YML))
    pairs=yaml.safe_load(open(PAIRS_CFG))
    return rb, pairs

def bucket_policy(riskbud:dict, bucket:str) -> dict: return riskbud["buckets"][bucket]
def deployable_from_live(kite)->float|None:
    try:
        m=kite.margins("equity"); avail=float(m.get("available",{}).get("live_balance") or 0.0)
        return avail*LIVE_USAGE_RATIO
    except Exception:
        return None

def load_token():
    import json as pyjson
    if not os.path.exists(TOKEN_FILE): return None
    t=pyjson.load(open(TOKEN_FILE))
    return (t.get("api_key"), t.get("access_token"))

async def main():
    riskbud, pairs_cfg = load_cfgs()
    risk_bucket = pairs_cfg["exec"].get("risk_bucket","MED")
    per_trade_inr = float(pairs_cfg["exec"].get("per_trade_inr", 10000))

    # optional Zerodha funds (for better sizing if available)
    live_cash=None
    try:
        from kiteconnect import KiteConnect
        ak, tok = load_token() or (None,None)
        if ak and tok and ak==os.getenv("KITE_API_KEY"):
            kite=KiteConnect(api_key=ak); kite.set_access_token(tok)
            live_cash = deployable_from_live(kite)
    except Exception:
        pass

    # DB (for later OMS integration if needed)
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)

    consumer = AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False, auto_offset_reset="earliest",
        group_id="pairs_meanrev",
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    print(f"[pairs-strat] IN={IN_TOPIC} → OUT={OUT_TOPIC} bucket={risk_bucket} per_trade_inr={per_trade_inr} live_cash={live_cash}")

    # local pos tracker: pair_id -> (qtyA, qtyB)
    positions={}

    async def send_order(sym, side, qty, strat, coid, ts):
        order={"client_order_id": f"{coid}:{sym}",
               "ts": int(ts), "symbol": sym, "side": side, "qty": int(qty), "order_type":"MKT",
               "strategy": strat, "risk_bucket": risk_bucket, "status":"NEW",
               "extra": {"reason": "pairs_meanrev"}}
        await producer.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=sym.encode())

    try:
        while True:
            batches=await consumer.getmany(timeout_ms=500, max_records=500)
            for _tp, msgs in batches.items():
                for m in msgs:
                    s=m.value
                    pid=int(s["pair_id"]); a=s["a_symbol"]; b=s["b_symbol"]; beta=float(s["beta"])
                    z=float(s["z"]); action=s["action"]; ts=int(s["ts"])
                    strat="PAIRS_MR"

                    # sizing: equal notional split across legs, hedge by beta
                    # pick cash basis
                    deployable = live_cash if live_cash and live_cash>0 else per_trade_inr
                    notional = max(deployable, 0.0)
                    if notional<=0: continue
                    half = notional/2.0
                    # we need latest prices; for simplicity, carry them inside signal? if not, assume c≈notional placeholder — better skip if missing
                    # In practice pass prices via signal; here's safe default:
                    pxA = None; pxB = None
                    # if producer didn't include prices, skip sizing; (recommend to add cA,cB fields in producer)
                    if "pxA" in s and "pxB" in s:
                        pxA=float(s["pxA"]); pxB=float(s["pxB"])
                    else:
                        # cannot size without prices; skip
                        continue
                    qtyA = int(max(1, half / max(pxA,1)))
                    qtyB = int(max(1, (half / max(pxB,1)) * abs(beta)))

                    coid=f"PAIR:{pid}:{action}:{ts}"

                    if action.startswith("ENTER_"):
                        if pid in positions:  # already in pos, ignore
                            continue
                        if "LONG_A_SHORT_B" in action:
                            await send_order(a, "BUY",  qtyA, strat, coid, ts)
                            await send_order(b, "SELL", qtyB, strat, coid, ts)
                            positions[pid]=(qtyA, -qtyB)
                        else:  # SHORT_A_LONG_B
                            await send_order(a, "SELL", qtyA, strat, coid, ts)
                            await send_order(b, "BUY",  qtyB, strat, coid, ts)
                            positions[pid]=(-qtyA, qtyB)
                    elif action.startswith("EXIT_"):
                        if pid not in positions:  # nothing to flatten
                            continue
                        qA,qB = positions.pop(pid)
                        # invert to flatten
                        await send_order(a, "BUY" if qA<0 else "SELL", abs(qA), strat, coid, ts)
                        await send_order(b, "BUY" if qB<0 else "SELL", abs(qB), strat, coid, ts)
            await consumer.commit()
    finally:
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
