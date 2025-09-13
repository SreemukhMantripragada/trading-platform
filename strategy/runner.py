"""
strategy/runner.py
- Consumes bars.* from Kafka
- Optional filter: configs/next_day.yaml -> activate only listed (symbol,strategy,tf,params)
- Sizes orders using live broker cash (80% use; 20% safety) + bucket splits
- Emits orders -> 'orders'

UNCHANGED ENV (plus NEXT_DAY):
  NEXT_DAY=configs/next_day.yaml   # optional; if missing, runs full registry
"""
from __future__ import annotations
import os, asyncio, importlib, yaml, ujson as json
from typing import Dict, Any, Tuple
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg

BROKER      = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC    = os.getenv("IN_TOPIC", "bars.1m")
OUT_TOPIC   = os.getenv("OUT_TOPIC", "orders")
GROUP_ID    = os.getenv("KAFKA_GROUP", "strat_runner")

REGISTRY_YAML   = os.getenv("STRAT_REGISTRY", "strategy/registry.yaml")
RISK_BUDGET_YML = os.getenv("RISK_BUDGET", "configs/risk_budget.yaml")
COSTS_YML       = os.getenv("COSTS_CFG", "configs/costs.yaml")
NEXT_DAY_PATH   = os.getenv("NEXT_DAY", "configs/next_day.yaml")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

LIVE_USAGE_RATIO = float(os.getenv("LIVE_USAGE_RATIO", "0.80"))  # 80%

# -------- utils --------
def _import_class(module_path: str, class_name: str):
    mod = importlib.import_module(module_path); return getattr(mod, class_name)

def _tf_from_topic(topic:str)->int:
    # bars.1m -> 1 ; bars.5m -> 5 ; bars.1s -> 1 (treat as 1m not supported)
    try:
        suf = topic.split(".")[1]
        return int(suf.replace("m","").replace("s","1"))
    except Exception:
        return 1

def load_registry() -> list[dict]:
    with open(REGISTRY_YAML) as f: return yaml.safe_load(f)["strategies"]

def load_riskbud() -> dict:
    with open(RISK_BUDGET_YML) as f: return yaml.safe_load(f)

def load_costs() -> dict:
    with open(COSTS_YML) as f: return yaml.safe_load(f)

def load_nextday(tf:int) -> tuple[set[str], dict[Tuple[str,str,tuple], dict]]:
    """
    Returns:
      allowed_symbols: set[str]
      wanted: { (strategy_name, symbol, params_items_tuple) : params_dict }
    If file missing/empty -> no restriction.
    """
    if not os.path.exists(NEXT_DAY_PATH):
        return set(), {}
    doc = yaml.safe_load(open(NEXT_DAY_PATH)) or {}
    allowed=set(); wanted={}
    for row in (doc.get("use") or []):
        if int(row.get("tf", -1)) != tf: continue
        sym=str(row["symbol"]); strat=str(row["strategy"]); params=row.get("params") or {}
        allowed.add(sym)
        key=(strat, sym, tuple(sorted(params.items())))
        wanted[key]=params
    return allowed, wanted

# ----- sizing helpers (kept from previous slice) -----
def bucket_policy(riskbud:dict, bucket:str) -> dict: return riskbud["buckets"][bucket]
def risk_per_trade(riskbud:dict, bucket:str) -> float: return float(riskbud["risk_per_trade"][bucket])
def cap_no_fixed_sl(riskbud:dict, bucket:str) -> float: return float(riskbud["no_fixed_sl_cap"][bucket])
def clamp_qty_by_cash(px: float, qty: int, cash_cap: float) -> int:
    if px <= 0 or cash_cap <= 0 or qty <= 0: return 0
    max_qty = int(cash_cap // px); return max(0, min(qty, max_qty))

async def main():
    # PG for live cash
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    from libs.cash import CashRefresher; cash = CashRefresher(pool, usage_ratio=LIVE_USAGE_RATIO, max_stale_sec=120)

    # configs
    reg = load_registry(); riskbud=load_riskbud(); costs_cfg=load_costs()
    tf=_tf_from_topic(IN_TOPIC)
    allowed_syms, wanted = load_nextday(tf)

    # build active strategy objects
    strategies=[]
    for s in reg:
        sname=s["name"]; Strat=_import_class(s["module"], s["class"])
        # If next_day present → instantiate only if strategy appears at least once in wanted
        if wanted:
            # we instantiate once per unique params that appear in wanted for this strategy
            unique_params=set(k[2] for k in wanted.keys() if k[0]==sname)
            for p_items in unique_params:
                params=dict(p_items)
                obj=Strat(**(params or {}))
                strategies.append({"obj":obj,"name":sname,"bucket":s.get("bucket","MED"),"params":params})
        else:
            obj=Strat(**(s.get("params") or {}))
            strategies.append({"obj":obj,"name":sname,"bucket":s.get("bucket","MED"),"params":(s.get("params") or {})})

    # cash refresher
    async def refresh_cash_task():
        while True:
            try:
                await cash.refresh()
                if cash.live_avail is not None:
                    print(f"[runner] live cash avail≈₹{cash.live_avail:.2f} → deployable≈₹{cash.deployable(riskbud):.2f}")
            except Exception as e:
                print(f"[runner] cash refresh error: {e}")
            await asyncio.sleep(15)
    refresher = asyncio.create_task(refresh_cash_task())

    # kafka
    consumer = AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False, auto_offset_reset="earliest",
        group_id=GROUP_ID, value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    print(f"[runner] IN={IN_TOPIC} TF={tf}m → OUT={OUT_TOPIC}; next_day={'ON' if wanted else 'OFF'}; strategies={len(strategies)}")

    from libs.costs import estimate_order_costs

    def current_deployable_cash() -> float: return cash.deployable(riskbud)
    def size_for_order(px: float, stop: float|None, bucket: str, symbol: str) -> int:
        dpl=current_deployable_cash(); pol=bucket_policy(riskbud,bucket)
        bucket_cash=dpl*float(pol["split"]); sym_cap=bucket_cash*float(pol["max_per_symbol"])
        if stop is not None and stop>0 and abs(px-stop)>1e-8:
            r=risk_per_trade(riskbud,bucket)
            qty_risk=int(max(0, r / max(abs(px-stop),1e-6) / max(px,1)))
            return clamp_qty_by_cash(px, qty_risk, sym_cap)
        money=min(sym_cap, cap_no_fixed_sl(riskbud,bucket))
        return clamp_qty_by_cash(px, int(money // max(px,1)), sym_cap)

    try:
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=2000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    r=m.value; sym=r["symbol"]; ts=int(r["ts"])
                    if wanted:
                        if allowed_syms and sym not in allowed_syms: continue
                    o,h,l,c,vol=float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"]),int(r["vol"])
                    for s in strategies:
                        strat=s["obj"]; sname=s["name"]; bucket=s["bucket"]; params=s["params"]
                        # If next_day is ON, ensure (sname,sym,params) is whitelisted
                        if wanted:
                            key=(sname, sym, tuple(sorted(params.items())))
                            if key not in wanted: continue
                        sig=strat.on_bar(sym, f"{tf}m", ts, o,h,l,c,vol, {})
                        if sig.side not in ("BUY","SELL","EXIT"): continue
                        qty=size_for_order(c, sig.stop_loss, bucket, sym)
                        if qty<=0: continue
                        est=estimate_order_costs(side=sig.side, price=c, qty=qty, bucket=bucket, costs_cfg=costs_cfg)
                        coid=f"{sname}:{sym}:{ts}:{sig.side}"
                        order={
                            "client_order_id": coid, "ts": ts, "symbol": sym, "side": sig.side, "qty": qty, "order_type": "MKT",
                            "strategy": sname, "risk_bucket": bucket, "status": "NEW",
                            "extra": {"strength": sig.strength, "stop_loss": sig.stop_loss, "take_profit": sig.take_profit,
                                      "costs_est": est, "reason": sig.reason or "", "params": params}
                        }
                        await producer.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=sym.encode())
            await consumer.commit()
    finally:
        refresher.cancel()
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
