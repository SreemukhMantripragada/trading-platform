# execution/stop_target_engine.py
from __future__ import annotations
import os, asyncio, time
import asyncpg, ujson as json
from aiokafka import AIOKafkaProducer

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders")
SCAN_SEC=int(os.getenv("STOP_SCAN_SEC","2"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

SQL_OPEN = """
WITH pos AS (
  SELECT symbol,
         SUM(CASE WHEN side='BUY'  THEN qty ELSE 0 END) -
         SUM(CASE WHEN side='SELL' THEN qty ELSE 0 END) AS net_qty
  FROM fills WHERE ts::date = CURRENT_DATE GROUP BY symbol
)
SELECT lo.client_order_id, lo.symbol, lo.side, lo.qty, lo.strategy, lo.risk_bucket, lo.extra,
       COALESCE( (SELECT c FROM bars_1m b WHERE b.symbol=lo.symbol ORDER BY ts DESC LIMIT 1), 0.0) AS last_c,
       (SELECT p.net_qty FROM pos p WHERE p.symbol=lo.symbol) AS net_qty
FROM live_orders lo
WHERE lo.status IN ('FILLED','ACK')  -- entries; you can refine by tagging entry/exit in extra
"""

def flip(side:str)->str: return "SELL" if side=="BUY" else "BUY"

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await prod.start()
    print(f"[stops] scanning every {SCAN_SEC}s; emitting exits to {OUT_TOPIC}")
    try:
        while True:
            try:
                async with pool.acquire() as con:
                    rows = await con.fetch(SQL_OPEN)
                for r in rows:
                    sym=r["symbol"]; last=float(r["last_c"] or 0.0)
                    extra=r["extra"] or {}
                    stop=float(extra.get("stop_px") or 0.0)
                    target=float(extra.get("target_px") or 0.0)
                    side=r["side"]

                    breach=False; reason=None
                    if side=="BUY":
                        if stop>0 and last <= stop: breach=True; reason=f"STOP {last:.2f}<= {stop:.2f}"
                        elif target>0 and last >= target: breach=True; reason=f"TARGET {last:.2f}>= {target:.2f}"
                    else:
                        if stop>0 and last >= stop: breach=True; reason=f"STOP {last:.2f}>= {stop:.2f}"
                        elif target>0 and last <= target: breach=True; reason=f"TARGET {last:.2f}<= {target:.2f}"

                    net = int(r["net_qty"] or 0)
                    if breach and net != 0:
                        qty=abs(net)
                        coid=f"EXIT-{int(time.time())}-{sym}"
                        order={
                            "client_order_id": coid,
                            "symbol": sym,
                            "side": flip(side),
                            "qty": qty,
                            "price": 0.0,
                            "strategy": "STOP_ENGINE",
                            "risk_bucket": r["risk_bucket"] or "MED",
                            "created_at": int(time.time()),
                            "extra": {"reason": reason}
                        }
                        await prod.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=sym.encode())
            except Exception as e:
                print("[stops] error:", e)
            await asyncio.sleep(SCAN_SEC)
    finally:
        await prod.stop()
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
