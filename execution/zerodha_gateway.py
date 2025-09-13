"""
Zerodha canary gateway (live/dry-run) with real fill polling:
- On place: writes broker_orders(coid, broker_order_id, status='PENDING', raw)
- Poll task every 4–8s: kite.orders() -> update broker_orders & OMS; emit real fill (est=False) when COMPLETE
"""
from __future__ import annotations
import os, time, asyncio, asyncpg, ujson as json, random
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from kiteconnect import KiteConnect, exceptions as kz_ex

from execution.oms import OMS
from libs.zerodha_map import ZMap

load_dotenv(".env"); load_dotenv("infra/.env")

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
ORD_TOPIC=os.getenv("ORD_TOPIC","orders")
FILL_TOPIC=os.getenv("FILL_TOPIC","fills")
GROUP_ID=os.getenv("KAFKA_GROUP","gateway_live")

KITE_API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

CANARY_BUCKETS=set((os.getenv("CANARY_BUCKETS","MED") or "MED").replace(" ","").split(","))
DRY_RUN = os.getenv("DRY_RUN","1") == "1"
QPS=float(os.getenv("QPS","3"))
PRODUCT=os.getenv("PRODUCT","MIS")

def _load_access_token() -> str:
    import json as pyjson
    t=pyjson.load(open(TOKEN_FILE))
    if t.get("api_key") != KITE_API_KEY: raise SystemExit("Token file API key mismatch.")
    return t["access_token"]

async def main():
    access=_load_access_token()
    kite=KiteConnect(api_key=KITE_API_KEY); kite.set_access_token(access)
    try: prof=kite.profile(); print(f"[gw] live={not DRY_RUN}; user={prof.get('user_id')}")
    except kz_ex.TokenException: raise SystemExit("Zerodha token invalid/expired.")

    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    oms=OMS(pool); zmap=ZMap("configs/tokens.csv")

    consumer = AIOKafkaConsumer(
        ORD_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
        auto_offset_reset="earliest", group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()

    # throttle
    min_interval = 1.0 / max(QPS, 0.1); last_t=0.0
    async def maybe_sleep():
        nonlocal last_t
        now=time.time(); delta=now-last_t
        if delta<min_interval: await asyncio.sleep(min_interval-delta)
        last_t=time.time()

    # --- POLLER: convert estimated -> real fills --------------------------------
    async def poll_fills():
        while True:
            try:
                if DRY_RUN:
                    await asyncio.sleep(6 + random.random()*2); continue
                ords=kite.orders()  # all orders today
                # index by order_id
                by_id={o.get("order_id"): o for o in ords if o.get("order_id")}
                # fetch pending broker_orders
                async with pool.acquire() as con:
                    rows=await con.fetch("SELECT coid, broker_order_id, status FROM broker_orders WHERE status IS NULL OR status IN ('PENDING','OPEN')")
                for r in rows:
                    coid=r["coid"]; oid=r["broker_order_id"]
                    O=by_id.get(oid)
                    if not O: continue
                    status=(O.get("status") or "").upper()
                    avg=O.get("average_price"); filled=O.get("filled_quantity")
                    raw=O
                    async with pool.acquire() as con:
                        await con.execute(
                          "UPDATE broker_orders SET status=$1, avg_price=$2, filled_qty=$3, raw=$4::jsonb, updated_at=now() WHERE coid=$5",
                          status, float(avg or 0.0), int(filled or 0), json.dumps(raw), coid
                        )
                    # transitions
                    if status=="COMPLETE":
                        # fetch OMS order to enrich
                        od = await oms.get_order(coid)
                        if od:
                            f={
                              "ts": int(time.time()), "symbol": od["symbol"], "side": od["side"], "qty": int(filled or od["qty"]),
                              "price": float(avg or 0.0), "fees_inr": 0.0, "client_order_id": coid,
                              "strategy": od["strategy"], "risk_bucket": od["risk_bucket"], "source": "zerodha_gateway", "order_id": oid,
                              "est": False
                            }
                            await producer.send_and_wait(FILL_TOPIC, json.dumps(f).encode(), key=od["symbol"].encode())
                            await oms.transition(coid, "FILLED", note="broker COMPLETE", meta={"order_id": oid, "px": f["price"]})
                    elif status in ("REJECTED","CANCELLED"):
                        await oms.transition(coid, status, note=f"broker {status}")
            except Exception as e:
                print(f"[gw/poll] {e}")
            await asyncio.sleep(6 + random.random()*2)

    poll_task=asyncio.create_task(poll_fills())

    print(f"[gw] consuming {ORD_TOPIC} → placing (dry_run={DRY_RUN}) for buckets {','.join(CANARY_BUCKETS)}")
    try:
        while True:
            batches=await consumer.getmany(timeout_ms=500, max_records=500)
            for _tp,msgs in batches.items():
                for m in msgs:
                    o=m.value; coid=o["client_order_id"]
                    if o.get("risk_bucket") not in CANARY_BUCKETS: continue
                    sym=o["symbol"]; side=o["side"].upper()
                    if side=="EXIT": side="SELL"
                    await oms.upsert_new(o); await oms.transition(coid, "ACK", note="accepted by Zerodha gateway")
                    info=zmap.resolve(sym)
                    if not info:
                        await oms.transition(coid, "REJECTED", note="symbol not in tokens.csv"); continue
                    await maybe_sleep()
                    try:
                        if DRY_RUN:
                            order_id=f"DRY-{int(time.time()*1000)}"
                            resp={"order_id": order_id}
                        else:
                            tr_type="BUY" if side=="BUY" else "SELL"
                            resp=kite.place_order(
                                variety=kite.VARIETY_REGULAR,
                                exchange=info["exchange"], tradingsymbol=info["tradingsymbol"],
                                transaction_type=tr_type, quantity=int(o["qty"]),
                                order_type=kite.ORDER_TYPE_MARKET, product=PRODUCT
                            )
                            order_id=resp.get("order_id","?")
                        # record broker_orders row (PENDING)
                        async with pool.acquire() as con:
                            await con.execute(
                              "INSERT INTO broker_orders(coid, broker_order_id, status, raw) VALUES($1,$2,$3,$4::jsonb) "
                              "ON CONFLICT (coid) DO UPDATE SET broker_order_id=EXCLUDED.broker_order_id, status=EXCLUDED.status, raw=EXCLUDED.raw, updated_at=now()",
                              coid, resp.get("order_id","?"), "PENDING", json.dumps(resp)
                            )
                        # emit conservative est fill immediately (so PnL charts move)
                        est=(o.get("extra") or {}).get("costs_est") or {}
                        px=float(est.get("est_fill_price") or 0.0); fees=float(est.get("est_total_cost_inr") or 0.0)
                        f={"ts": int(time.time()), "symbol": sym, "side": side, "qty": int(o["qty"]), "price": px,
                           "fees_inr": fees, "client_order_id": coid, "strategy": o["strategy"], "risk_bucket": o["risk_bucket"],
                           "source":"zerodha_gateway", "order_id": resp.get("order_id","?"), "est": True}
                        await producer.send_and_wait(FILL_TOPIC, json.dumps(f).encode(), key=sym.encode())
                    except kz_ex.KiteException as e:
                        await oms.transition(coid, "REJECTED", note=str(e))
                    except Exception as e:
                        await oms.transition(coid, "REJECTED", note=f"unknown: {e}")
            await consumer.commit()
    finally:
        poll_task.cancel()
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
