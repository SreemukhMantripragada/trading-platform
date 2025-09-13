import os, asyncio, time, ujson as json, yaml
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
from prometheus_client import start_http_server, Counter

# ---- Env ----
BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("IN_TOPIC", "orders")
OUT_TOPIC = os.getenv("OUT_TOPIC", "orders.sized")
GROUP_ID  = os.getenv("KAFKA_GROUP", "risk_manager")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

RISK_CONF = os.getenv("RISK_CONF", "configs/risk.yaml")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8008"))

# ---- Metrics ----
ORD_SEEN   = Counter("risk_orders_seen_total", "Orders seen by risk manager")
ORD_OK     = Counter("risk_orders_approved_total", "Orders approved/sized")
ORD_REJ    = Counter("risk_orders_rejected_total", "Orders rejected by risk")

# ---- SQL ----
SQL_SEL_ORDER = "SELECT order_id, symbol, side, qty, status FROM orders WHERE client_order_id=$1"
SQL_LATEST_P  = "SELECT c FROM bars_1m WHERE symbol=$1 ORDER BY ts DESC LIMIT 1"
SQL_COUNT_OPEN= "SELECT count(*) AS n FROM positions WHERE qty <> 0"
SQL_UPD_ORDER = """
UPDATE orders
SET qty = $2,
    status = $3,
    extra = COALESCE(extra,'{}'::jsonb) || $4::jsonb
WHERE order_id = $1
"""

def load_config(path):
    with open(path) as f: return yaml.safe_load(f)

def size_order(conf, price, req_qty, open_pos_count):
    # caps
    min_qty = max(1, int(conf.get("min_qty", 1)))
    max_qty_per_sym = int(conf.get("per_symbol_max_qty", 1_000_000))
    per_order_notional = float(conf.get("per_order_notional_cap", 1e12))
    max_open_positions = int(conf.get("max_open_positions", 10))

    if open_pos_count >= max_open_positions:
        return 0, "max_open_positions"

    # shrink by per-order notional
    max_qty_by_notional = int(per_order_notional // max(price, 0.01))
    sized = min(req_qty, max_qty_per_sym, max_qty_by_notional)

    if sized < min_qty:
        return 0, "min_qty/notional"

    return sized, "ok"

async def main():
    conf = load_config(RISK_CONF)
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
                    msg = m.value
                    coid = msg.get("client_order_id")
                    if not coid: continue

                    async with pool.acquire() as con:
                        async with con.transaction():
                            row = await con.fetchrow(SQL_SEL_ORDER, coid)
                            if not row:
                                # strategy didn't persist → ignore
                                continue
                            oid, sym, side, qty, status = int(row["order_id"]), row["symbol"], row["side"], int(row["qty"]), row["status"]
                            if status in ("APPROVED","FILLED","REJECTED","CANCELED"):
                                # already processed
                                continue
                            # price ref
                            pr = await con.fetchrow(SQL_LATEST_P, sym)
                            price = float(pr["c"]) if pr and pr["c"] is not None else 100.0
                            # open positions
                            open_pos = int((await con.fetchrow(SQL_COUNT_OPEN))["n"])
                            # size
                            sized_qty, reason = size_order(conf, price, qty, open_pos)
                            if sized_qty <= 0:
                                ORD_REJ.inc()
                                risk_blob = json.dumps({"risk":{"action":"REJECT","reason":reason,"ref_price":price}})
                                await con.execute(SQL_UPD_ORDER, oid, qty, "REJECTED", risk_blob)
                                continue

                            # approve + resize
                            ORD_OK.inc()
                            risk_blob = json.dumps({"risk":{"action":"APPROVE","sized_qty":sized_qty,"ref_price":price}})
                            await con.execute(SQL_UPD_ORDER, oid, sized_qty, "APPROVED", risk_blob)

                            # forward to next hop
                            fwd = dict(msg)
                            fwd["approved_qty"] = sized_qty
                            fwd["approved_price_ref"] = price
                            await producer.send_and_wait(OUT_TOPIC, json.dumps(fwd).encode(), key=sym.encode())

            await consumer.commit()
    finally:
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__ == "__main__":
    asyncio.run(main())