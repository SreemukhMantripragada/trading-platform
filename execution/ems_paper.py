"""
execution/ems_paper.py
- Consumes 'orders' from Kafka
- ACKs in OMS
- Tries C++ matcher; if not available, fills immediately at est price
- Emits 'fills' to Kafka
- Rate-limits outbound (basic)

ENV:
  KAFKA_BROKER=localhost:9092
  ORD_TOPIC=orders
  FILL_TOPIC=fills
  KAFKA_GROUP=ems_paper
  POSTGRES_HOST/PORT/DB/USER/PASSWORD
  MATCHER_HOST=127.0.0.1
  MATCHER_PORT=5556
  USE_MATCHER=0|1
"""
from __future__ import annotations
import os, time, asyncio, asyncpg, ujson as json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from typing import Dict, Any
from execution.oms import OMS
from execution.matcher_client import MatcherClient

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
ORD_TOPIC=os.getenv("ORD_TOPIC","orders")
FILL_TOPIC=os.getenv("FILL_TOPIC","fills")
GROUP_ID=os.getenv("KAFKA_GROUP","ems_paper")
USE_MATCHER = os.getenv("USE_MATCHER","0") == "1"
MATCHER_HOST=os.getenv("MATCHER_HOST","127.0.0.1")
MATCHER_PORT=int(os.getenv("MATCHER_PORT","5556"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

RATE_LIMIT_QPS=float(os.getenv("EMS_QPS","20"))  # crude limiter

async def main():
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    oms = OMS(pool)

    consumer = AIOKafkaConsumer(
        ORD_TOPIC,
        bootstrap_servers=BROKER,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)

    matcher = MatcherClient(MATCHER_HOST, MATCHER_PORT) if USE_MATCHER else None
    if matcher:
        try:
            await matcher.connect()
            print("[ems] connected to matcher")
        except Exception as e:
            print(f"[ems] matcher connect failed: {e}; falling back to instant fills")
            matcher = None

    await consumer.start(); await producer.start()
    print(f"[ems] {ORD_TOPIC} → {FILL_TOPIC} (matcher={'on' if matcher else 'off'})")

    last_sent=0.0
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=2000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    o: Dict[str,Any] = m.value
                    coid=o["client_order_id"]

                    # 1) ensure OMS NEW exists (idempotent) and ACK it
                    await oms.upsert_new(o)
                    await oms.transition(coid, "ACK", note="accepted by EMS")

                    # 2) throttle a bit
                    now=time.time()
                    min_interval=1.0/max(1e-6,RATE_LIMIT_QPS)
                    if now - last_sent < min_interval:
                        await asyncio.sleep(min_interval - (now - last_sent))
                    last_sent=time.time()

                    # 3) route to matcher or instant fill
                    try:
                        if matcher:
                            await matcher.send_order(coid, o["symbol"], o["side"], int(o["qty"]))
                            # naive: wait a short time for a single fill line
                            fill=None
                            try:
                                fill = await asyncio.wait_for(matcher._r.readline(), timeout=0.25)
                                if fill:
                                    fill = json.loads(fill.decode())
                            except asyncio.TimeoutError:
                                fill=None
                            if fill is None:
                                # fallback instant fill
                                raise RuntimeError("matcher no immediate fill")
                            px=float(fill.get("price") or o["extra"].get("costs_est",{}).get("est_fill_price") or 0.0)
                        else:
                            px=float(o["extra"].get("costs_est",{}).get("est_fill_price") or 0.0)
                        # 4) emit one full fill (paper)
                        f={
                          "ts": int(time.time()),
                          "symbol": o["symbol"],
                          "side": o["side"],
                          "qty": int(o["qty"]),
                          "price": px,
                          "fees_inr": float(o["extra"].get("costs_est",{}).get("est_total_cost_inr", 0.0)),
                          "client_order_id": coid,
                          "strategy": o["strategy"],
                          "risk_bucket": o["risk_bucket"]
                        }
                        await producer.send_and_wait(FILL_TOPIC, json.dumps(f).encode(), key=o["symbol"].encode())
                        await oms.transition(coid, "FILLED", note="paper fill", meta={"px":px,"qty":f["qty"]})
                    except Exception as e:
                        await oms.transition(coid, "REJECTED", note=str(e))
            await consumer.commit()
    finally:
        await consumer.stop(); await producer.stop()
        if matcher: await matcher.close()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
