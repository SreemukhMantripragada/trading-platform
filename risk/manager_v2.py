# risk/manager_v2.py
import os, asyncio, time, ujson as json, yaml, asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from typing import Dict, Any
from brokers.zerodha_account import Account
from libs.killswitch import KillSwitch

from prometheus_client import start_http_server, Counter, Gauge, Histogram
APPROVED=Counter("risk_orders_approved_total","approved") 
REJECTED=Counter("risk_orders_rejected_total","rejected")
PROCESS_TIME = Histogram(
    "risk_manager_process_seconds",
    "Time spent evaluating an order",
    ["result"],
    buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0),
)
ORDER_LAG = Histogram(
    "risk_manager_order_lag_seconds",
    "Latency between order timestamp and risk decision",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60),
)
FUNDS_TRADABLE = Gauge("risk_manager_tradable_equity_inr", "Tradable equity reported by broker adapter")
FUNDS_TOTAL = Gauge("risk_manager_equity_inr", "Total equity reported by broker adapter")

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","orders")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders.sized")
GROUP_ID=os.getenv("KAFKA_GROUP","risk_manager_v2")
PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
CONF=os.getenv("RISK_CONF","configs/risk_budget.yaml")
KS=KillSwitch()
LEVERAGE_MULT = float(os.getenv("PAIRS_LEVERAGE", os.getenv("PAIRS_BT_LEVERAGE", "5.0")))

def load_conf(path)->Dict[str,Any]:
    return yaml.safe_load(open(path))

def pos_size(entry, stop, risk_rupees, tick):
    edge=max(entry - stop, tick)
    return max(0, int(risk_rupees/edge))

async def latest_close(pool, sym):
    async with pool.acquire() as con:
        r=await con.fetchrow("SELECT c FROM bars_1m WHERE symbol=$1 ORDER BY ts DESC LIMIT 1", sym)
    return float(r["c"]) if r and r["c"] is not None else 100.0

async def main():
    conf=load_conf(CONF)
    acc=Account()  # broker adapter
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    cons=AIOKafkaConsumer(IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
        auto_offset_reset="earliest", group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await cons.start(); await prod.start()
    start_http_server(int(os.getenv("METRICS_PORT","8116")))
    try:
        while True:
            if KS.is_tripped()[0]: print("[KILL] risk stop"); break

            # refresh available budget periodically
            try:
                funds = await acc.tradable_equity(conf)
            except Exception as exc:
                if not globals().get("_RISK_FALLBACK_WARNED"):
                    print(f"[risk-manager] tradable_equity fetch failed ({exc}); using configured fallback")
                    globals()["_RISK_FALLBACK_WARNED"] = True
                total_cash = float(conf.get("total_cash_inr", 0.0) or 0.0)
                holdback = float(conf.get("holdback_fraction", 0.0) or 0.0)
                tradable = total_cash * max(0.0, 1.0 - holdback)
                funds = {"tradable": tradable, "equity": total_cash}
            try:
                FUNDS_TRADABLE.set(float(funds.get("tradable", 0.0)))
            except (TypeError, ValueError):
                FUNDS_TRADABLE.set(0.0)
            try:
                FUNDS_TOTAL.set(float(funds.get("equity", 0.0)))
            except (TypeError, ValueError):
                FUNDS_TOTAL.set(0.0)

            batches=await cons.getmany(timeout_ms=1000, max_records=500)
            for _tp, msgs in batches.items():
                for m in msgs:
                    o=m.value
                    start_proc = time.perf_counter()
                    result = "skipped"
                    try:
                        if o.get("status")!="NEW":
                            continue
                        result = "reviewed"
                        order_ts_raw = o.get("ts")
                        try:
                            order_ts = int(order_ts_raw)
                        except (TypeError, ValueError):
                            order_ts = None
                        if order_ts is not None:
                            ORDER_LAG.observe(max(0.0, time.time() - order_ts))
                        sym = o["symbol"]
                        bucket = o.get("risk_bucket", "LOW")
                        extra = (o.get("extra") or {})
                        leverage_raw = extra.get("leverage")
                        try:
                            leverage = float(leverage_raw if leverage_raw is not None else LEVERAGE_MULT)
                        except (TypeError, ValueError):
                            leverage = LEVERAGE_MULT
                        if leverage <= 0:
                            leverage = 1.0
                        reason = str(extra.get("reason") or "").upper()
                        if reason == "EXIT" or extra.get("pair_exit"):
                            qty = int(o.get("qty") or 0)
                            if qty <= 0:
                                continue
                            sized = dict(o)
                            sized["status"] = "APPROVED"
                            sized["qty"] = qty
                            sized_extra = dict(extra)
                            risk_block = dict((sized_extra.get("risk") or {}))
                            risk_block.update({"qty": qty, "mode": "exit_passthrough"})
                            sized_extra["risk"] = risk_block
                            sized["extra"] = sized_extra
                            async with pool.acquire() as con:
                                await con.execute(
                                    "UPDATE orders SET qty=$2, status='APPROVED', extra=COALESCE(extra,'{}'::jsonb)||$3::jsonb WHERE client_order_id=$1",
                                    o["client_order_id"], qty, json.dumps({"risk":{"qty":qty,"mode":"exit_passthrough","leverage":leverage}})
                                )
                                APPROVED.inc()
                            await prod.send_and_wait(OUT_TOPIC, json.dumps(sized).encode(), key=sym.encode())
                            result = "approved"
                            continue
                        bucket_conf = (conf.get("buckets") or {}).get(bucket)
                        if isinstance(bucket_conf, dict):
                            bucket_pct = float(bucket_conf.get("split", 0.0))
                        else:
                            bucket_pct = float(bucket_conf or 0.0)
                        bucket_budget = funds["tradable"] * bucket_pct
                        tick = float(conf.get("tick_size", 0.05))
                        ref = await latest_close(pool, sym)

                        sig = (o.get("extra") or {}).get("signal") or {}
                        entry = float(sig.get("entry_px") or ref)
                        stop = sig.get("stop_px")
                        if stop is not None:
                            stop = float(stop)
                            risk_pct_map = conf.get("risk_per_trade_pct") or {}
                            risk_pct_raw = risk_pct_map.get(bucket, risk_pct_map.get("DEFAULT", risk_pct_map.get("default", 0.0)))
                            try:
                                risk_pct = float(risk_pct_raw)
                            except (TypeError, ValueError):
                                risk_pct = 0.0
                            risk_rupees = funds["equity"] * (risk_pct / 100.0)
                            risk_rupees *= leverage
                            qty = pos_size(entry, stop, risk_rupees, tick)
                        else:
                            caps_map = conf.get("monetary_caps") or {}
                            cap = float(caps_map.get(o.get("strategy"), 0))
                            qty = int((cap * leverage) / max(ref, 0.01))

                        per_sym_max = int(conf.get("per_symbol_max_qty", 10_000))
                        notional = qty * entry
                        cash_notional = notional / leverage if leverage > 0 else notional
                        if cash_notional > bucket_budget:
                            qty = int((bucket_budget * leverage) / max(entry, 0.01))
                        qty = max(0, min(qty, per_sym_max))

                        if qty <= 0:
                            async with pool.acquire() as con:
                                await con.execute(
                                    "UPDATE orders SET status='REJECTED', extra=COALESCE(extra,'{}'::jsonb)||$2::jsonb WHERE client_order_id=$1",
                                    o["client_order_id"], json.dumps({"risk":{"reason":"insufficient_budget"}})
                                )
                                REJECTED.inc()
                            result = "rejected"
                            continue

                        sized = dict(o)
                        sized["qty"] = qty
                        sized["status"] = "APPROVED"
                        sized_extra = dict(sized.get("extra") or {})
                        risk_block = dict((sized_extra.get("risk") or {}))
                        risk_block.update({"qty": qty, "entry_ref": entry, "bucket_budget": bucket_budget, "leverage": leverage})
                        sized_extra["risk"] = risk_block
                        sized["extra"] = sized_extra

                        async with pool.acquire() as con:
                            await con.execute(
                                "UPDATE orders SET qty=$2, status='APPROVED', extra=COALESCE(extra,'{}'::jsonb)||$3::jsonb WHERE client_order_id=$1",
                                o["client_order_id"], qty, json.dumps({"risk":{"qty":qty,"leverage":leverage}})
                            )
                            APPROVED.inc()
                        await prod.send_and_wait(OUT_TOPIC, json.dumps(sized).encode(), key=sym.encode())
                        result = "approved"
                    finally:
                        PROCESS_TIME.labels(result).observe(max(0.0, time.perf_counter() - start_proc))
            await cons.commit()
    finally:
        await cons.stop(); await prod.stop(); await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
