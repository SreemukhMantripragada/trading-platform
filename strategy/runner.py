# strategy/runner.py
from __future__ import annotations
import os, asyncio, importlib, yaml, ujson as json
from typing import Dict, Any, Tuple, List, Set
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
from dotenv import load_dotenv

# --- Load env files ---
# priority: explicit env > infra/.env > .env
load_dotenv("infra/.env", override=False)
load_dotenv(".env", override=False)

# --- Core configs from env (with fallbacks) ---
BROKER      = os.getenv("KAFKA_BROKER", "localhost:9092")

# allow multiple topics
IN_TOPICS   = os.getenv("KAFKA_IN_TOPICS", "").strip()
IN_TOPIC    = os.getenv("KAFKA_IN_TOPIC", os.getenv("IN_TOPIC", "bars.3m")).strip()
OUT_TOPIC   = os.getenv("KAFKA_OUT_TOPIC", "orders").strip()
GROUP_ID    = os.getenv("KAFKA_GROUP_ID", "strat_runner").strip()

REGISTRY_YAML   = os.getenv("STRAT_REGISTRY", "configs/registry.yaml")
RISK_BUDGET_YML = os.getenv("RISK_BUDGET", "configs/risk_budget.yaml")
COSTS_YML       = os.getenv("COSTS_CFG", "configs/costs.yaml")
NEXT_DAY_PATH   = os.getenv("NEXT_DAY", "configs/next_day.yaml")

PG_HOST=os.getenv("POSTGRES_HOST", "localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB=os.getenv("POSTGRES_DB", "trading")
PG_USER=os.getenv("POSTGRES_USER", "postgres")
PG_PASS=os.getenv("POSTGRES_PASSWORD", "postgres")

LIVE_USAGE_RATIO = float(os.getenv("LIVE_USAGE_RATIO", "0.8"))


# -------- utils --------
def _import_class(module_path: str, class_name: str):
    mod = importlib.import_module(module_path); return getattr(mod, class_name)

def _tf_from_topic(topic:str)->int:
    # bars.3m -> 3 ; bars.5m -> 5 ; bars.15m -> 15
    try:
        suf = topic.split(".", 1)[1]
        if suf.endswith("m"):
            return int(suf[:-1])
        # default: treat unknown as 1
        return 1
    except Exception:
        return 1

def load_registry() -> List[dict]:
    with open(REGISTRY_YAML) as f: return yaml.safe_load(f)["strategies"]

def load_riskbud() -> dict:
    with open(RISK_BUDGET_YML) as f: return yaml.safe_load(f)

def load_costs() -> dict:
    with open(COSTS_YML) as f: return yaml.safe_load(f)

def _wanted_from_nextday_doc(doc: dict, tf: int) -> tuple[Set[str], dict[Tuple[str,str,tuple], dict]]:
    """
    Support both schemas:

    A) legacy selections:
       selections:
         - symbol: HDFCBANK
           tf_min: 5
           entries:
             - strategy: EMA_CROSS
               params: {fast: 12, slow: 100}
               variant: ...

    B) simple use list:
       use:
         - symbol: HDFCBANK
           strategy: EMA_CROSS
           tf: 5
           params: {...}
    """
    allowed: Set[str] = set()
    wanted: Dict[Tuple[str,str,tuple], dict] = {}

    # (A) selections schema
    sels = doc.get("selections")
    if isinstance(sels, list):
        for bucket in sels:
            if int(bucket.get("tf_min", -1)) != tf: 
                continue
            sym = str(bucket.get("symbol", "")).strip()
            if not sym: 
                continue
            allowed.add(sym)
            for ent in (bucket.get("entries") or []):
                strat = str(ent.get("strategy","")).strip()
                params = ent.get("params") or {}
                key = (strat, sym, tuple(sorted(params.items())))
                wanted[key] = params

    # (B) use schema
    uses = doc.get("use")
    if isinstance(uses, list):
        for row in uses:
            if int(row.get("tf", -1)) != tf:
                continue
            sym = str(row.get("symbol","")).strip()
            strat = str(row.get("strategy","")).strip()
            if not sym or not strat:
                continue
            params = row.get("params") or {}
            allowed.add(sym)
            key = (strat, sym, tuple(sorted(params.items())))
            wanted[key] = params

    return allowed, wanted

def load_nextday_for_tf(tf:int) -> tuple[Set[str], dict[Tuple[str,str,tuple], dict]]:
    if not NEXT_DAY_PATH or not os.path.exists(NEXT_DAY_PATH):
        return set(), {}
    doc = yaml.safe_load(open(NEXT_DAY_PATH)) or {}
    return _wanted_from_nextday_doc(doc, tf)

# ----- sizing helpers (unchanged) -----
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

    # Topics to consume
    topics = [t.strip() for t in IN_TOPICS.split(",") if t.strip()] or [IN_TOPIC]
    tfs = {t: _tf_from_topic(t) for t in topics}

    # next_day filters per timeframe
    wanted_by_tf: Dict[int, Tuple[Set[str], Dict[Tuple[str,str,tuple], dict]]] = {}
    for tf in sorted(set(tfs.values())):
        wanted_by_tf[tf] = load_nextday_for_tf(tf)
    nextday_on = any(w for (_, w) in wanted_by_tf.values())

    # build active strategy objects
    # NOTE: one shared instance per strategy/params (as in your original file)
    strategies=[]
    for s in reg:
        sname=s["name"]; Strat=_import_class(s["module"], s["class"])
        default_params = s.get("params") or {}
        bucket = s.get("bucket","MED")

        # If next_day is ON → instantiate once per unique params that appear in wanted for this strategy (across TFs)
        if nextday_on:
            unique_params = set()
            for tf, (allowed, wanted) in wanted_by_tf.items():
                for (nm, _sym, p_items) in wanted.keys():
                    if nm == sname:
                        unique_params.add(p_items)
            if unique_params:
                for p_items in unique_params:
                    params=dict(p_items)
                    obj=Strat(**(params or {}))
                    strategies.append({"obj":obj,"name":sname,"bucket":bucket,"params":params})
                continue  # done for this strategy if we instantiated from next_day

        # Fallback: instantiate with default params (unfiltered run)
        obj=Strat(**default_params)
        strategies.append({"obj":obj,"name":sname,"bucket":bucket,"params":default_params})

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
        *topics,
        bootstrap_servers=BROKER,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id=GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    pretty_topics = ",".join(topics)
    print(f"[runner] IN={pretty_topics} → OUT={OUT_TOPIC}; next_day={'ON' if nextday_on else 'OFF'}; strategies={len(strategies)}")

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
            for tp, msgs in batches.items():
                tf = _tf_from_topic(tp.topic)
                allowed_syms, wanted = wanted_by_tf.get(tf, (set(), {}))
                for m in msgs:
                    r=m.value; sym=str(r["symbol"]); ts=int(r["ts"])
                    if wanted:
                        if allowed_syms and sym not in allowed_syms:
                            continue
                    o,h,l,c,vol=float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"]),int(r["vol"])
                    for s in strategies:
                        strat=s["obj"]; sname=s["name"]; bucket=s["bucket"]; params=s["params"]
                        # If next_day is ON, ensure (sname,sym,params) is whitelisted for THIS tf
                        if wanted:
                            key=(sname, sym, tuple(sorted(params.items())))
                            if key not in wanted: 
                                continue
                        # KEEPING YOUR PUBLIC API: strategy.on_bar(sym, "3m"/"5m"/"15m", ts, o,h,l,c,vol, extra)
                        sig=strat.on_bar(sym, f"{tf}m", ts, o,h,l,c,vol, {})

                        if sig.side not in ("BUY","SELL","EXIT"):
                            continue
                        qty=size_for_order(c, sig.stop_loss, bucket, sym)
                        if qty<=0:
                            continue
                        est=estimate_order_costs(side=sig.side, price=c, qty=qty, bucket=bucket, costs_cfg=costs_cfg)
                        coid=f"{sname}:{sym}:{ts}:{sig.side}"
                        order={
                            "client_order_id": coid, "ts": ts, "symbol": sym, "side": sig.side, "qty": qty, "order_type": "MKT",
                            "strategy": sname, "risk_bucket": bucket, "status": "NEW",
                            "extra": {"strength": sig.strength, "stop_loss": sig.stop_loss, "take_profit": sig.take_profit,
                                      "costs_est": est, "reason": sig.reason or "", "params": params, "tf": tf}
                        }
                        await producer.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=sym.encode())
            await consumer.commit()
    finally:
        refresher.cancel()
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
