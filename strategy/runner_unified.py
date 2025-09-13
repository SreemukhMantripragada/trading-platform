"""
strategy/runner_unified.py
- Loads registry.yaml (leafs then ensembles)
- Optional filter via NEXT_DAY (configs/next_day.yaml)
- Sizing uses live cash (80% default) + bucket splits from risk_budget.yaml
- Emits orders -> Kafka 'orders'
- Kill switch aware

ENV (common):
  KAFKA_BROKER, IN_TOPIC=bars.1m, OUT_TOPIC=orders, STRAT_REGISTRY, RISK_BUDGET, COSTS_CFG, NEXT_DAY
"""

from __future__ import annotations
import os, asyncio, importlib, yaml, ujson as json, time
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg

from libs.killswitch import KillSwitch
from strategy.base import BaseStrategy, Signal

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","bars.1m")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders")
GROUP_ID=os.getenv("KAFKA_GROUP","runner_unified")

REGISTRY_YAML=os.getenv("STRAT_REGISTRY","strategy/registry.yaml")
RISK_BUDGET_YML=os.getenv("RISK_BUDGET","configs/risk_budget.yaml")
COSTS_YML=os.getenv("COSTS_CFG","configs/costs.yaml")
NEXT_DAY_PATH=os.getenv("NEXT_DAY","configs/next_day.yaml")
LIVE_USAGE_RATIO=float(os.getenv("LIVE_USAGE_RATIO","0.80"))

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

def _import_class(module_path: str, class_name: str):
    mod=importlib.import_module(module_path); return getattr(mod, class_name)

def load_yaml(p): 
    with open(p) as f: return yaml.safe_load(f)

def load_nextday(tf_int: int):
    if not os.path.exists(NEXT_DAY_PATH): return set(), {}
    doc = load_yaml(NEXT_DAY_PATH) or {}
    allowed=set(); wanted={}
    for row in (doc.get("use") or []):
        if int(row.get("tf", -1)) != tf_int: continue
        sym=str(row["symbol"]); strat=str(row["strategy"]); params=row.get("params") or {}
        allowed.add(sym)
        wanted[(strat, sym, tuple(sorted(params.items())))] = params
    return allowed, wanted

def tf_from_topic(topic:str)->tuple[int,str]:
    # "bars.1m" -> (1,"1m"), "bars.5m" -> (5,"5m"), "bars.1s" -> (1,"1s")
    suf=topic.split(".")[-1]
    if suf.endswith("m"): return int(suf[:-1]), suf
    if suf.endswith("s"): return 1, suf
    return 1, "1m"

# ---- sizing helpers (reuse your policy) ----
def bucket_policy(riskbud:dict, bucket:str) -> dict: return riskbud["buckets"][bucket]
def risk_per_trade(riskbud:dict, bucket:str) -> float: return float(riskbud["risk_per_trade"][bucket])
def cap_no_fixed_sl(riskbud:dict, bucket:str) -> float: return float(riskbud["no_fixed_sl_cap"][bucket])
def clamp_qty_by_cash(px: float, qty: int, cash_cap: float) -> int:
    if px <= 0 or cash_cap <= 0 or qty <= 0: return 0
    max_qty = int(cash_cap // px); return max(0, min(qty, max_qty))

async def main():
    KS = KillSwitch()

    # DB + live cash helper
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    from libs.cash import CashRefresher; cash = CashRefresher(pool, usage_ratio=LIVE_USAGE_RATIO, max_stale_sec=120)

    riskbud = load_yaml(RISK_BUDGET_YML)
    costs_cfg = load_yaml(COSTS_YML)
    tf_int, tf_str = tf_from_topic(IN_TOPIC)
    allowed_syms, wanted = load_nextday(tf_int)

    reg = load_yaml(REGISTRY_YAML)["strategies"]

    # 1) instantiate leafs first
    name_to_obj: Dict[str, BaseStrategy] = {}
    strat_defs_leaf = [s for s in reg if s.get("type","leaf")=="leaf"]
    for s in strat_defs_leaf:
        Cls = _import_class(s["module"], s["class"])
        obj = Cls(**(s.get("params") or {}))
        name_to_obj[s["name"]] = obj
    # 2) ensembles next (inject children)
    strat_defs_ens = [s for s in reg if s.get("type")=="ensemble"]
    for s in strat_defs_ens:
        Cls = _import_class(s["module"], s["class"])
        child_names = s["params"].pop("children")
        children = [(n, name_to_obj[n]) for n in child_names]
        obj = Cls(children=children, **(s.get("params") or {}))
        name_to_obj[s["name"]] = obj

    # Effective run-list (name, obj, bucket, params)
    runlist=[]
    for s in reg:
        obj = name_to_obj[s["name"]]
        runlist.append({"name": s["name"], "obj": obj, "bucket": s.get("bucket","MED"), "params": s.get("params") or {}})

    # live cash refresher task
    async def refresh_cash():
        while True:
            try: await cash.refresh()
            except Exception as e: print(f"[runner] cash refresh err: {e}")
            await asyncio.sleep(15)
    asyncio.create_task(refresh_cash())

    # kafka io
    consumer=AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False, auto_offset_reset="earliest",
        group_id=GROUP_ID, value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    print(f"[runner] TF={tf_str} leaf={len(strat_defs_leaf)} ens={len(strat_defs_ens)} next_day={'ON' if wanted else 'OFF'}")

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
            # kill switch check
            tripped, msg = KS.is_tripped()
            if tripped:
                print(f"[KILL] stopping runner: {msg}"); break

            batches=await consumer.getmany(timeout_ms=500, max_records=2000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    r=m.value; sym=r["symbol"]; ts=int(r["ts"])
                    if wanted and allowed_syms and sym not in allowed_syms: continue
                    o,h,l,c,vol=float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"]),int(r["vol"])
                    for s in runlist:
                        name=s["name"]; obj=s["obj"]; bucket=s["bucket"]; params=s["params"]
                        if wanted and (name, sym, tuple(sorted(params.items()))) not in wanted:
                            # when NEXT_DAY is on, only whitelisted tuples run
                            continue
                        # warmup guard
                        if getattr(obj, "warmup_bars", lambda tf:0)(tf_str) > 0:
                            # (we skip explicit tracking; child maintains internal state)
                            pass
                        sig: Signal = obj.on_bar(sym, tf_str, ts, o,h,l,c,vol, {})
                        if sig.side not in ("BUY","SELL","EXIT"): continue
                        qty=size_for_order(c, sig.stop_loss, bucket, sym)
                        if qty<=0: continue
                        est=estimate_order_costs(side=sig.side, price=c, qty=qty, bucket=bucket, costs_cfg=costs_cfg)
                        coid=f"{name}:{sym}:{ts}:{sig.side}"
                        order={
                            "client_order_id": coid, "ts": ts, "symbol": sym, "side": sig.side, "qty": qty, "order_type": "MKT",
                            "strategy": name, "risk_bucket": bucket, "status": "NEW",
                            "extra": {"strength": sig.strength, "stop_loss": sig.stop_loss, "take_profit": sig.take_profit,
                                      "costs_est": est, "reason": sig.reason or "", "params": params}
                        }
                        await producer.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=sym.encode())
            await consumer.commit()
    finally:
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
