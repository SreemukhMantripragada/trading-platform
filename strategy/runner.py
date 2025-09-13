import os, asyncio, time, ujson as json
from collections import deque, defaultdict
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
from prometheus_client import start_http_server, Counter

# ---------- Config via env ----------
BROKER     = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC   = os.getenv("IN_TOPIC", "bars.1m")
OUT_TOPIC  = os.getenv("OUT_TOPIC", "orders")
GROUP_ID   = os.getenv("KAFKA_GROUP", "strategy_runner_sma")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8006"))
DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))
RISK_BUCKET = os.getenv("RISK_BUCKET", "LOW")     # future: dynamic risk manager

# ---------- Metrics ----------
INTENTS = Counter("strategy_intents_total", "Signals emitted")
ORDERS  = Counter("orders_emitted_total", "Orders pushed to Kafka+DB")

# ---------- Strategy base ----------
class BaseStrategy:
    name = "Base"
    def on_bar(self, symbol: str, ts_sec: int, o: float, h: float, l: float, c: float, vol: int):
        """Return one of: +1 (go long), -1 (go short/sell), 0/None (no action)."""
        raise NotImplementedError

# ---------- Child: SMA crossover ----------
class SMA_Cross(BaseStrategy):
    name = "SMA_Cross"
    def __init__(self, fast:int=5, slow:int=20):
        assert fast < slow, "fast must be < slow"
        self.fast = fast
        self.slow = slow
        self.prices = defaultdict(lambda: deque(maxlen=slow))
        self.sum_fast = defaultdict(float)
        self.sum_slow = defaultdict(float)
        self.last_state = defaultdict(lambda: 0)  # -1 below, +1 above, 0 unknown

    def _sma(self, sym):
        dq = self.prices[sym]
        n = len(dq)
        if n < self.slow: return None, None
        # rolling sums: fast = last fast elements; slow = all slow
        slow_sum = sum(dq)                      # small N, OK
        fast_sum = sum(list(dq)[-self.fast:])
        return (fast_sum / self.fast), (slow_sum / self.slow)

    def on_bar(self, symbol, ts_sec, o,h,l,c,vol):
        dq = self.prices[symbol]
        dq.append(float(c))
        fast, slow = self._sma(symbol)
        if fast is None:    # not enough history
            return None
        state = 1 if fast > slow else (-1 if fast < slow else 0)
        prev  = self.last_state[symbol]
        self.last_state[symbol] = state
        if prev <= 0 and state > 0:
            return +1   # golden cross -> buy signal
        if prev >= 0 and state < 0:
            return -1   # death cross -> sell signal
        return None

# ---------- Runner ----------
async def ensure_db(pool):
    # tables were created in Step 10; nothing to do; keep function for future migrations
    return

def mk_client_id(strategy:str, symbol:str, ts_sec:int) -> str:
    return f"{strategy}:{symbol}:{ts_sec}"

async def main():
    start_http_server(METRICS_PORT)
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    await ensure_db(pool)

    consumer = AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
        auto_offset_reset="earliest", group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)

    strat = SMA_Cross(fast=5, slow=20)

    await consumer.start(); await producer.start()
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=1000, max_records=2000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    r = m.value
                    # expected payload from 1m aggregator: symbol, tf='1m', ts, o,h,l,c,vol
                    sym = r["symbol"]; ts = int(r["ts"])
                    o,h,l,c = float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"])
                    vol = int(r.get("vol") or 0)

                    sig = strat.on_bar(sym, ts, o,h,l,c,vol)  # +1/-1/None
                    if sig is None:
                        continue

                    INTENTS.inc()
                    side = "BUY" if sig > 0 else "SELL"
                    qty  = DEFAULT_QTY
                    coid = mk_client_id(strat.name, sym, ts)

                    order = {
                        "client_order_id": coid,
                        "ts": ts,
                        "symbol": sym,
                        "side": side,
                        "qty": qty,
                        "order_type": "MKT",
                        "strategy": strat.name,
                        "reason": "SMA crossover",
                        "risk_bucket": RISK_BUCKET,
                        "status": "NEW",
                    }

                    # 1) write to DB (orders)
                    async with pool.acquire() as con:
                        await con.execute(
                            "INSERT INTO orders(client_order_id, ts, symbol, side, qty, order_type, strategy, reason, risk_bucket, status) "
                            "VALUES($1, to_timestamp($2), $3, $4, $5, $6, $7, $8, $9, $10) "
                            "ON CONFLICT (client_order_id) DO NOTHING",
                            order["client_order_id"], order["ts"], order["symbol"], order["side"],
                            order["qty"], order["order_type"], order["strategy"], order["reason"],
                            order["risk_bucket"], order["status"]
                        )

                    # 2) publish to Kafka 'orders'
                    await producer.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=sym.encode())
                    ORDERS.inc()

            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())