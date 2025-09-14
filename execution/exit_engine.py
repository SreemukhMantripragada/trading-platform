# execution/exit_engine.py
from __future__ import annotations
import os, asyncio, time, ujson as json
from datetime import datetime, timezone, timedelta
from aiokafka import AIOKafkaProducer
import asyncpg
from accounting.position_tracker import PositionTracker

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders")
PORTFOLIO_SCAN_SEC=int(os.getenv("EXIT_SCAN_SEC","5"))
IST = timezone(timedelta(hours=5, minutes=30))

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

async def ist_now():
    return datetime.now(timezone.utc).astimezone(IST)

def ist_minutes(dt:datetime)->int:
    return dt.hour*60 + dt.minute

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    ptrack=PositionTracker(pool)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await prod.start()
    print(f"[exit] scanning every {PORTFOLIO_SCAN_SEC}s; publishing exits to {OUT_TOPIC}")
    try:
        while True:
            try:
                await ptrack.refresh()
                now_ist = await ist_now()
                mins = ist_minutes(now_ist)

                # Auto-flatten near close (15:15 IST)
                if mins >= (15*60 + 15) and mins <= (15*60 + 30):
                    for sym,(net,_) in list(ptrack.pos.items()):
                        if net == 0: continue
                        side = "SELL" if net > 0 else "BUY"
                        coid = f"FLATTEN-{int(time.time())}-{sym}"
                        order = {
                            "client_order_id": coid,
                            "symbol": sym,
                            "side": side,
                            "qty": abs(net),
                            "price": 0.0,
                            "strategy": "EXIT_ENGINE",
                            "risk_bucket": "MED",
                            "created_at": int(time.time()),
                            "extra": {"reason":"EOD_FLATTEN"}
                        }
                        await prod.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=sym.encode())

                # TODO: stop/target: if you persist per-position stop_px/target_px in live_orders.extra,
                # query them and compare against latest price (bars_1m.c) then emit exit order similarly.

            except Exception as e:
                print("[exit] error:", e)
            await asyncio.sleep(PORTFOLIO_SCAN_SEC)
    finally:
        await prod.stop()
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
