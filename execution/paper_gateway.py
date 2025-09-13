import os, asyncio, time, ujson as json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
from prometheus_client import start_http_server, Counter, Histogram

# ---- Env ----
BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("IN_TOPIC", "orders")
OUT_TOPIC = os.getenv("OUT_TOPIC", "fills")
GROUP_ID  = os.getenv("KAFKA_GROUP", "paper_exec")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

SLIPPAGE_BPS = float(os.getenv("SLIPPAGE_BPS", "5"))   # 5 bps = 0.05%
LATENCY_MS   = int(os.getenv("FILL_LATENCY_MS", "50")) # simulate small delay
METRICS_PORT = int(os.getenv("METRICS_PORT", "8007"))

# ---- Metrics ----
ORD_SEEN   = Counter("exec_orders_seen_total", "Orders seen by paper gateway")
ORD_FILLED = Counter("exec_orders_filled_total", "Orders filled by paper gateway")
ORD_SKIP   = Counter("exec_orders_skipped_total", "Orders skipped (already filled)")
FILL_LAT   = Histogram("exec_fill_latency_seconds", "Latency order_ts->fill")

# ---- SQL helpers ----
SQL_SELECT_ORDER = """
SELECT order_id, symbol, side, qty, ts
FROM orders WHERE client_order_id = $1
"""

SQL_LATEST_CLOSE = """
SELECT c FROM bars_1m WHERE symbol = $1 ORDER BY ts DESC LIMIT 1
"""

SQL_SUM_FILLED = """
SELECT COALESCE(SUM(qty),0) AS filled FROM fills WHERE order_id = $1
"""

SQL_INS_FILL = """
INSERT INTO fills(order_id, ts, qty, price, venue, extra)
VALUES($1, now(), $2, $3, 'PAPER', '{}'::jsonb)
RETURNING fill_id
"""

SQL_UPD_ORDER_STATUS = """
UPDATE orders SET status = $2 WHERE order_id = $1
"""

SQL_UPSERT_POS_BUY = """
INSERT INTO positions(symbol, qty, avg_price, realized_pnl, updated_at)
VALUES($1, $2, $3, 0, now())
ON CONFLICT(symbol) DO UPDATE SET
  qty = positions.qty + EXCLUDED.qty,
  avg_price = CASE
     WHEN positions.qty + EXCLUDED.qty = 0 THEN 0
     ELSE (positions.avg_price * positions.qty + EXCLUDED.avg_price * EXCLUDED.qty)
          / NULLIF(positions.qty + EXCLUDED.qty, 0)
  END,
  updated_at = now()
"""

SQL_UPD_POS_SELL = """
-- realize PnL on sell; reduce qty; keep avg_price if qty remains, else reset to 0
WITH cur AS (
  SELECT qty, avg_price FROM positions WHERE symbol = $1 FOR UPDATE
)
INSERT INTO positions(symbol, qty, avg_price, realized_pnl, updated_at)
VALUES($1, -$2, 0, ($3 - 0) * 0, now())  -- if no prior pos, PnL baseline 0
ON CONFLICT(symbol) DO UPDATE SET
  realized_pnl = positions.realized_pnl + 
                 (CASE 
                    WHEN GREATEST(LEAST(positions.qty, $2), 0) IS NULL THEN 0
                    ELSE ( $3 - positions.avg_price ) * GREATEST(LEAST(positions.qty, $2), 0)
                  END),
  qty = positions.qty - $2,
  avg_price = CASE WHEN positions.qty - $2 = 0 THEN 0 ELSE positions.avg_price END,
  updated_at = now()
"""

def apply_slippage(side:str, ref_price:float, bps:float)->float:
    # BUY worse, SELL worse
    mult = (bps/10000.0)
    if side == "BUY":
        return round(ref_price * (1.0 + mult), 2)
    else:
        return round(ref_price * (1.0 - mult), 2)

async def main():
    start_http_server(METRICS_PORT)
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)

    consumer = AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
        auto_offset_reset="earliest", group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()

    try:
        while True:
            batches = await consumer.getmany(timeout_ms=1000, max_records=500)
            for _tp, msgs in batches.items():
                for m in msgs:
                    ORD_SEEN.inc()
                    order = m.value
                    coid  = order["client_order_id"]
                    # small simulated latency
                    await asyncio.sleep(LATENCY_MS/1000.0)

                    async with pool.acquire() as con:
                        async with con.transaction():
                            row = await con.fetchrow(SQL_SELECT_ORDER, coid)
                            if not row:
                                # strategy wrote to Kafka but not to DB (shouldn't happen)
                                continue
                            order_id, symbol, side, qty, ts = int(row["order_id"]), row["symbol"], row["side"], int(row["qty"]), row["ts"]

                            # idempotency: skip if fully filled
                            filled = int((await con.fetchrow(SQL_SUM_FILLED, order_id))["filled"])
                            if filled >= qty:
                                ORD_SKIP.inc()
                                continue

                            # price reference = latest 1m close
                            pr = await con.fetchrow(SQL_LATEST_CLOSE, symbol)
                            ref = float(pr["c"]) if pr and pr["c"] is not None else 100.0
                            price = apply_slippage(side, ref, SLIPPAGE_BPS)

                            # fill remaining qty
                            fill_qty = qty - filled
                            await con.fetchval(SQL_INS_FILL, order_id, fill_qty, price)

                            # mark as filled
                            await con.execute(SQL_UPD_ORDER_STATUS, order_id, "FILLED")

                            # update positions
                            if side == "BUY":
                                await con.execute(SQL_UPSERT_POS_BUY, symbol, fill_qty, price)
                            else:
                                await con.execute(SQL_UPD_POS_SELL, symbol, fill_qty, price)

                    # emit fill to Kafka
                    now_sec = int(time.time())
                    fill_msg = {"order_id": order_id, "symbol": symbol, "qty": fill_qty, "price": price, "ts": now_sec, "venue":"PAPER"}
                    await producer.send_and_wait(OUT_TOPIC, json.dumps(fill_msg).encode(), key=symbol.encode())

                    # metrics latency (order ts -> now)
                    if isinstance(order.get("ts"), (int,float)):
                        FILL_LAT.observe(max(0.0, time.time() - float(order["ts"])))
                    ORD_FILLED.inc()

            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
