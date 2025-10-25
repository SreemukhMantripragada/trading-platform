# execution/paper_gateway_matcher.py
import os, asyncio, time, ujson as json, asyncpg
from aiokafka import AIOKafkaConsumer
from execution.costs import CostModel
from execution.matcher_client import MatcherClient
from prometheus_client import start_http_server, Counter, Histogram
FILLS=Counter("exec_orders_filled_total","fills")
PROCESS_TIME = Histogram(
    "paper_matcher_process_seconds",
    "Time to simulate execution for an order",
    ["side"],
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2),
)
ORDER_LAG = Histogram(
    "paper_matcher_order_lag_seconds",
    "Latency between order timestamp and simulated fill",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30),
)
BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","orders.sized")
GROUP_ID=os.getenv("KAFKA_GROUP","paper_exec_matcher")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

async def last_price(pool, sym):
    async with pool.acquire() as con:
        r=await con.fetchrow("SELECT c FROM bars_1m WHERE symbol=$1 ORDER BY ts DESC LIMIT 1", sym)
    return float(r["c"]) if r and r["c"] is not None else 100.0

async def main():
    costs=CostModel()
    mc=MatcherClient(host=os.getenv("MATCHER_HOST","127.0.0.1"), port=int(os.getenv("MATCHER_PORT","5555")))
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    cons=AIOKafkaConsumer(IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
                          auto_offset_reset="earliest", group_id=GROUP_ID,
                          value_deserializer=lambda b: json.loads(b.decode()),
                          key_deserializer=lambda b: b.decode() if b else None)
    await cons.start()
    start_http_server(int(os.getenv("METRICS_PORT","8117")))
    try:
        while True:
            batches=await cons.getmany(timeout_ms=1000, max_records=500)
            for _tp, msgs in batches.items():
                for m in msgs:
                    o=m.value
                    if o.get("status")!="APPROVED":
                        continue
                    side=o["side"]
                    start_proc = time.perf_counter()
                    order_ts_raw = o.get("ts")
                    try:
                        order_ts = int(order_ts_raw)
                    except (TypeError, ValueError):
                        order_ts = None
                    sym=o["symbol"]; side=o["side"]; qty=int(o["qty"])
                    # route to C++ matcher (returns list of partial fills {qty, price})
                    ref = await last_price(pool, sym)
                    fills = await mc.execute(sym, side, qty, ref_price=ref)
                    # fills = await mc.execute(sym, side, qty)
                    async with pool.acquire() as con:
                        async with con.transaction():
                            # mark order SENT
                            await con.execute("UPDATE orders SET status='FILLED' WHERE client_order_id=$1", o["client_order_id"])
                            for f in fills:
                                fees = costs.legs_fees(side, int(f["qty"]), float(f["price"]))
                                await con.execute(
                                  "INSERT INTO fills(order_id, ts, qty, price, venue, extra) "
                                  "SELECT order_id, now(), $1, $2, 'MATCHER', $3 FROM orders WHERE client_order_id=$4",
                                  int(f["qty"]), float(f["price"]), json.dumps({"fees":fees}), o["client_order_id"]
                                )
                                FILLS.inc()
                    if order_ts is not None:
                        ORDER_LAG.observe(max(0.0, time.time() - order_ts))
                    PROCESS_TIME.labels(side).observe(max(0.0, time.perf_counter() - start_proc))
            await cons.commit()
    finally:
        await cons.stop(); await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
