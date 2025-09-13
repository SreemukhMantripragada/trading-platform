"""
Ledger consumer:
- Consumes 'fills' from Kafka
- Upserts positions with WAC (weighted average cost)
- Writes realized PnL & cash ledger movements
- Audits raw fills

ENV:
  KAFKA_BROKER=localhost:9092
  FILL_TOPIC=fills
  KAFKA_GROUP=ledger_consumer
  POSTGRES_HOST=localhost
  POSTGRES_PORT=5432
  POSTGRES_DB=trading
  POSTGRES_USER=trader
  POSTGRES_PASSWORD=trader
"""
from __future__ import annotations
import os, asyncio, asyncpg, ujson as json
from aiokafka import AIOKafkaConsumer

BROKER     = os.getenv("KAFKA_BROKER", "localhost:9092")
FILL_TOPIC = os.getenv("FILL_TOPIC", "fills")
GROUP_ID   = os.getenv("KAFKA_GROUP", "ledger_consumer")

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB  =os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

UPSERT_SQL = """
INSERT INTO positions(symbol, qty, avg_price, realized_pnl, updated_at)
VALUES($1,$2,$3,$4, now())
ON CONFLICT (symbol) DO UPDATE
SET qty=EXCLUDED.qty, avg_price=EXCLUDED.avg_price,
    realized_pnl=positions.realized_pnl + (EXCLUDED.realized_pnl - positions.realized_pnl) + 0, -- placeholder merge
    updated_at=now()
"""

INSERT_FILL_AUDIT = """
INSERT INTO fills_audit(ts, symbol, side, qty, price, fees_inr, client_order_id, strategy, risk_bucket, raw)
VALUES(to_timestamp($1), $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""

INSERT_LEDGER = """
INSERT INTO cash_ledger(ts, reason, amount, meta) VALUES (to_timestamp($1), $2, $3, $4)
"""

async def process_fill(con: asyncpg.Connection, r: dict):
    """
    Expected fill schema (minimal):
      {
        "ts": <epoch sec>,
        "symbol": "RELIANCE",
        "side": "BUY" | "SELL",
        "qty": 100,
        "price": 2500.50,
        "fees_inr": 12.34,
        "client_order_id": "...",
        "strategy": "...",
        "risk_bucket": "LOW"
      }
    """
    ts = int(r["ts"])
    sym = r["symbol"]
    side= r["side"].upper()
    qty = int(r["qty"])
    px  = float(r["price"])
    fees= float(r.get("fees_inr", 0.0))

    # Fetch current position
    pos = await con.fetchrow("SELECT qty, avg_price, realized_pnl FROM positions WHERE symbol=$1", sym)
    cur_qty   = int(pos["qty"]) if pos else 0
    cur_avg   = float(pos["avg_price"]) if pos else 0.0
    realized  = float(pos["realized_pnl"]) if pos else 0.0

    if side == "BUY":
        new_qty = cur_qty + qty
        # WAC update: new_avg = (cur_qty*cur_avg + qty*px) / new_qty
        new_avg = ((cur_qty * cur_avg) + (qty * px)) / max(new_qty, 1)
        realized_delta = 0.0
        cash_delta = - (qty * px) - fees  # cash out
        reason = "FILL_BUY"
    else:  # SELL
        new_qty = cur_qty - qty
        pnl_trade = (px - cur_avg) * qty
        realized_delta = pnl_trade
        new_avg = cur_avg if new_qty > 0 else 0.0
        cash_delta = (qty * px) - fees     # cash in
        reason = "FILL_SELL"

    await con.execute(UPSERT_SQL, sym, new_qty, new_avg, realized + realized_delta)
    await con.execute(INSERT_FILL_AUDIT, ts, sym, side, qty, px, fees,
                      r.get("client_order_id"), r.get("strategy"), r.get("risk_bucket"), json.dumps(r))
    await con.execute(INSERT_LEDGER, ts, reason, cash_delta, json.dumps({"symbol": sym, "qty": qty, "px": px, "fees": fees}))

async def main():
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    consumer = AIOKafkaConsumer(
        FILL_TOPIC,
        bootstrap_servers=BROKER,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    await consumer.start()
    print(f"[ledger] consuming fills from {FILL_TOPIC}")
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=2000)
            for _tp, msgs in batches.items():
                async with pool.acquire() as con:
                    async with con.transaction():
                        for m in msgs:
                            await process_fill(con, m.value)
            await consumer.commit()
    finally:
        await consumer.stop(); await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
