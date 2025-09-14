# risk/order_budget_guard.py
from __future__ import annotations
import os, asyncio, ujson as json, time
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server, Gauge, Counter
from risk.budget_source import compute_budgets

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","orders")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders.allowed")
GROUP_ID=os.getenv("KAFKA_GROUP","order_budget_guard")
PORT=int(os.getenv("METRICS_PORT","8023"))

ALLOC_USED = Gauge("budget_used_inr", "Committed notional per bucket", ["bucket"])
ORDERS_ALLOWED = Counter("orders_allowed_total", "Orders allowed", ["bucket"])
ORDERS_REJECT  = Counter("orders_rejected_total", "Orders rejected", ["bucket","reason"])

async def main():
    start_http_server(PORT)
    budgets = compute_budgets()  # {"LOW":{"budget":...,"max_per_trade":...}, ...}
    used = {"LOW":0.0,"MED":0.0,"HIGH":0.0}

    cons=AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, group_id=GROUP_ID, enable_auto_commit=False, auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await cons.start(); await prod.start()
    print(f"[guard] {IN_TOPIC} → {OUT_TOPIC} enforcing 80/20 reserve and bucket splits")

    try:
        async for m in cons:
            o=m.value
            try:
                bucket=(o.get("risk_bucket") or "MED").upper()
                bspec = budgets.get(bucket, {"budget":0.0,"max_per_trade":0.0})
                cap   = float(bspec["budget"])
                per   = float(bspec["max_per_trade"])

                px_ref = float(((o.get("extra") or {}).get("px_ref")) or 0.0)
                notional = px_ref * int(o["qty"])
                reason=None
                if notional <= 0:
                    reason="bad_notional"
                elif notional > per + 1e-6:
                    reason="exceeds_max_per_trade"
                elif used[bucket] + notional > cap + 1e-6:
                    reason="exceeds_bucket_budget"

                if reason:
                    ORDERS_REJECT.labels(bucket, reason).inc()
                else:
                    used[bucket]+=notional
                    ALLOC_USED.labels(bucket).set(used[bucket])
                    ORDERS_ALLOWED.labels(bucket).inc()
                    await prod.send_and_wait(OUT_TOPIC, json.dumps(o).encode(), key=(o.get("symbol") or "").encode())

                await cons.commit()
            except Exception as e:
                ORDERS_REJECT.labels((o.get("risk_bucket") or "MED").upper(), "exception").inc()
                print("[guard] error:", e)
                await cons.commit()
    finally:
        await cons.stop(); await prod.stop()

if __name__=="__main__":
    asyncio.run(main())
