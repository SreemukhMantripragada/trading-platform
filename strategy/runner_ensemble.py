import os, asyncio, ujson as json
from collections import deque, defaultdict, Counter as CC
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
from prometheus_client import start_http_server, Counter

BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("IN_TOPIC", "bars.1m")
OUT_TOPIC = os.getenv("OUT_TOPIC", "orders")
GROUP_ID  = os.getenv("KAFKA_GROUP", "ensemble_runner")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8009"))
DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))
RISK_BUCKET = os.getenv("RISK_BUCKET", "LOW")

INTENTS = Counter("ensemble_intents_total", "Ensemble signals")
ORDERS  = Counter("ensemble_orders_total", "Orders emitted by ensemble")

class BaseStrategy:
    name = "Base"
    def on_bar(self, symbol, ts, o,h,l,c,vol): raise NotImplementedError

class SMA_Cross(BaseStrategy):
    name = "SMA_Cross"
    def __init__(self, fast=5, slow=20):
        assert fast<slow
        self.fast, self.slow = fast, slow
        self.prices = defaultdict(lambda: deque(maxlen=slow))
        self.last_state = defaultdict(int)
    def on_bar(self, sym, ts, o,h,l,c,vol):
        dq = self.prices[sym]; dq.append(float(c))
        if len(dq) < self.slow: return None
        slow = sum(dq)/self.slow
        fast = sum(list(dq)[-self.fast:])/self.fast
        state = 1 if fast>slow else (-1 if fast<slow else 0)
        prev = self.last_state[sym]; self.last_state[sym]=state
        if prev<=0 and state>0: return +1
        if prev>=0 and state<0: return -1
        return None

class RSI_Reversion(BaseStrategy):
    name = "RSI_Reversion"
    def __init__(self, period=14, buy_th=30, sell_th=70):
        self.period=period; self.buy_th=buy_th; self.sell_th=sell_th
        self.closes = defaultdict(lambda: deque(maxlen=period+1))
    def _rsi(self, dq):
        if len(dq) < self.period+1: return None
        gains=losses=0.0
        for i in range(1,len(dq)):
            d = dq[i]-dq[i-1]
            if d>0: gains+=d
            elif d<0: losses+=-d
        avg_gain = gains/self.period
        avg_loss = losses/self.period
        if avg_loss==0: return 100.0
        rs = avg_gain/avg_loss
        return 100.0 - (100.0/(1.0+rs))
    def on_bar(self, sym, ts, o,h,l,c,vol):
        dq = self.closes[sym]; dq.append(float(c))
        r = self._rsi(dq)
        if r is None: return None
        if r < self.buy_th: return +1
        if r > self.sell_th: return -1
        return None

def mk_coid(strategy, symbol, ts): return f"{strategy}:{symbol}:{ts}"

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

    s1 = SMA_Cross(5,20)
    s2 = RSI_Reversion(14, 30, 70)

    await consumer.start(); await producer.start()
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=1000, max_records=2000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    r = m.value
                    sym = r["symbol"]; ts=int(r["ts"])
                    o,h,l,c = float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"])
                    vol = int(r.get("vol") or 0)

                    votes = []
                    for strat in (s1,s2):
                        s = strat.on_bar(sym, ts, o,h,l,c,vol)
                        if s is not None: votes.append(int(s))
                    if not votes: continue

                    INTENTS.inc()
                    score = sum(votes)
                    if score>0: side="BUY"
                    elif score<0: side="SELL"
                    else: continue

                    coid = mk_coid("Ensemble", sym, ts)
                    order = {
                        "client_order_id": coid,
                        "ts": ts,
                        "symbol": sym,
                        "side": side,
                        "qty": DEFAULT_QTY,
                        "order_type": "MKT",
                        "strategy": "Ensemble(SMA+RSI)",
                        "reason": f"votes={votes}",
                        "risk_bucket": "MED",
                        "status": "NEW",
                    }

                    # persist in DB so risk/exec can read
                    async with pool.acquire() as con:
                        await con.execute(
                            "INSERT INTO orders(client_order_id, ts, symbol, side, qty, order_type, strategy, reason, risk_bucket, status) "
                            "VALUES($1, to_timestamp($2), $3, $4, $5, $6, $7, $8, $9, $10) "
                            "ON CONFLICT (client_order_id) DO NOTHING",
                            order["client_order_id"], order["ts"], order["symbol"], order["side"],
                            order["qty"], order["order_type"], order["strategy"], order["reason"],
                            order["risk_bucket"], order["status"]
                        )
                    # publish
                    await producer.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=sym.encode())
                    ORDERS.inc()
            await consumer.commit()
    finally:
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
