"""
execution/pairs_executor.py
Turns pairs.signals into two-leg orders (enter/exit). Budget from risk_budget.runtime.yaml.

Signal message (pairs.signals):
{
  "pair_id":"REL_TCS", "a_symbol":"RELIANCE", "b_symbol":"TCS",
  "beta":0.8, "z":2.1, "action":"ENTER_LONG_A_SHORT_B",  // or ENTER_SHORT_A_LONG_B or EXIT
  "risk_bucket":"MED", "pxA":2901.5, "pxB":4102.0, "ts": 1736499999000
}

ENV:
  KAFKA_BROKER=localhost:9092
  IN_TOPIC=pairs.signals
  OUT_TOPIC=orders
  GROUP_ID=pairs_executor
  RISK_BUDGET=configs/risk_budget.runtime.yaml
  METRICS_PORT=8115
"""
from __future__ import annotations
import os, asyncio, ujson as json, time, yaml, asyncpg, math
from pathlib import Path
import json as pyjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from execution.throttle import Throttles
from execution.oms import OMS
from typing import Dict, Any, Tuple, Optional

BROKER   = os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC","pairs.signals")
OUT_TOPIC= os.getenv("OUT_TOPIC","orders")
GROUP_ID = os.getenv("GROUP_ID","pairs_executor")
BUDGET_YAML = os.getenv("RISK_BUDGET","configs/risk_budget.runtime.yaml")
METRICS_PORT= int(os.getenv("METRICS_PORT","8115"))
PAIRS_NEXT_DAY = os.getenv("PAIRS_NEXT_DAY","configs/pairs_next_day.yaml")
PAIRS_CFG_RELOAD_SEC = int(os.getenv("PAIRS_CFG_RELOAD_SEC","300"))
STATE_FILE = Path(os.getenv("PAIRS_STATE_FILE", "data/runtime/pairs_state.json"))
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

ORDERS_EMIT = Counter("pairs_orders_emitted_total","orders emitted",["pair_id","leg","bucket","action"])
SIGNALS_SEEN= Counter("pairs_signals_total","signals processed",["action"])
REJECTS     = Counter("pairs_rejects_total","signals rejected",["reason"])
PROCESS_TIME = Histogram(
    "pairs_executor_process_seconds",
    "Time spent handling a single signal",
    ["action"],
    buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1, 2),
)
SIGNAL_LAG = Histogram(
    "pairs_executor_signal_lag_seconds",
    "Latency between signal timestamp and executor processing",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60),
)
ORDER_PUBLISH_LAG = Histogram(
    "pairs_executor_order_publish_lag_seconds",
    "Latency between signal timestamp and order publish",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60),
)
OPEN_PAIRS = Gauge("pairs_executor_open_pairs", "Open pair positions tracked by executor")

def load_pairs_config(path: str) -> dict[Tuple[str,str], dict]:
    if not path or not os.path.exists(path):
        return {}
    try:
        doc = yaml.safe_load(open(path)) or {}
    except Exception:
        return {}
    out: dict[Tuple[str,str], dict] = {}
    for row in doc.get("selections", []) or []:
        sym = str(row.get("symbol","")).strip()
        if "-" not in sym:
            continue
        leg_a, leg_b = [s.strip().upper() for s in sym.split("-", 1)]
        if not leg_a or not leg_b:
            continue
        key = tuple(sorted((leg_a, leg_b)))
        stats = row.get("stats") or {}
        out[key] = {
            "symbol": sym,
            "bucket": str(row.get("bucket","MED")).upper(),
            "strategy": str(row.get("strategy") or "PAIRS"),
            "risk_per_trade": row.get("risk_per_trade"),
            "avg_hold_min": stats.get("avg_hold_min", row.get("avg_hold_min", 0.0)),
        }
    return out

def load_budget():
    # Expect:
    # buckets:
    #   MED: { split: 0.3, max_per_trade: 20000 }
    try:
        cfg=yaml.safe_load(open(BUDGET_YAML))
        return cfg.get("buckets",{})
    except Exception:
        return {"MED":{"split":0.3,"max_per_trade":20000}}

async def positions_today(pool, pair_id:str):
    """
    Reads today's net positions for the pair legs from fills.extra JSON.
    Requires fills.extra->>'pair_id' to be set by gateways.
    Returns dict { "A": net_qty, "B": net_qty }
    """
    async with pool.acquire() as con:
        rows = await con.fetch("""
          SELECT (extra->>'leg') AS leg,
                 SUM(CASE WHEN side='BUY' THEN qty ELSE -qty END)::int AS net
          FROM fills
          WHERE ts::date = CURRENT_DATE AND (extra->>'pair_id') = $1
          GROUP BY leg
        """, pair_id)
    out={"A":0,"B":0}
    for r in rows:
        if r["leg"] in ("A","B"):
            out[r["leg"]] = int(r["net"] or 0)
    return out

def size_legs(notional:float, pxA:float, pxB:float, beta:float):
    """
    Dollar-neutral sizing: weights 1 (A) and beta (B).
    qtyA ~ notional / (pxA + beta*pxB); qtyB ~ beta * qtyA
    """
    pxA=max(pxA,1e-6); pxB=max(pxB,1e-6); beta=max(beta,1e-6)
    denom = pxA + beta*pxB
    if denom <= 0: return 0,0
    qA = math.floor( (notional/denom) )
    qB = math.floor( beta * qA )
    return max(qA,0), max(qB,0)

def coid(pair_id:str, leg:str):
    return f"PAIR:{pair_id}:{leg}:{int(time.time()*1000)}"

def load_state() -> dict:
    try:
        data = pyjson.loads(STATE_FILE.read_text())
        if isinstance(data, dict):
            return data
    except FileNotFoundError:
        return {}
    except Exception as exc:
        print(f"[pairs-exec] state load error: {exc}")
    return {}

def save_state(state: dict) -> None:
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(pyjson.dumps(state, indent=2, sort_keys=True))
        tmp.replace(STATE_FILE)
    except Exception as exc:
        print(f"[pairs-exec] state save error: {exc}")

async def main():
    start_http_server(METRICS_PORT)
    buckets = load_budget()
    # simple throttles (symbol & bucket level)
    T = Throttles(sym_qps=1.0, bucket_qps={"LOW":1.0,"MED":0.7,"HIGH":0.4})

    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    oms = OMS(pool)
    consumer=AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, group_id=GROUP_ID, enable_auto_commit=False, auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode()), key_deserializer=lambda b: b.decode() if b else None
    )
    producer=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()

    pair_meta = load_pairs_config(PAIRS_NEXT_DAY)
    last_cfg_reload = time.time()
    print(f"[pairs-exec] {IN_TOPIC} → {OUT_TOPIC} using {BUDGET_YAML}; whitelist={'OFF' if not pair_meta else 'ON'}")
    positions_state = load_state()
    if positions_state:
        print(f"[pairs-exec] restored {len(positions_state)} open pair(s) from state file.")
    OPEN_PAIRS.set(len(positions_state))
    try:
        async for m in consumer:
            now = time.time()
            if now - last_cfg_reload >= PAIRS_CFG_RELOAD_SEC:
                pair_meta = load_pairs_config(PAIRS_NEXT_DAY)
                last_cfg_reload = now

            s = m.value
            start_proc = time.perf_counter()
            act = (s.get("action") or "").upper()
            act_label = act or "UNKNOWN"
            try:
                pair_id = s["pair_id"]
                a = str(s["a_symbol"])
                b = str(s["b_symbol"])
                beta = float(s.get("beta", 1.0))
                key = tuple(sorted((a.upper(), b.upper())))
                meta = pair_meta.get(key)
                bucket = (s.get("risk_bucket") or "MED").upper()
                strategy_name = "PAIRS"
                avg_hold = 0.0
                risk_override = None
                if meta:
                    bucket = meta.get("bucket", bucket).upper()
                    strategy_name = meta.get("strategy", "PAIRS")
                    avg_hold = float(meta.get("avg_hold_min") or 0.0)
                    ro = meta.get("risk_per_trade")
                    try:
                        risk_override = float(ro) if ro is not None else None
                    except Exception:
                        risk_override = None
                elif pair_meta and act_label.startswith("ENTER"):
                    REJECTS.labels("not_whitelisted").inc()
                    await consumer.commit()
                    continue

                pxA = float(s.get("pxA") or 0.0)
                pxB = float(s.get("pxB") or 0.0)
                max_per_trade = float(buckets.get(bucket, {}).get("max_per_trade", 20000.0))
                SIGNALS_SEEN.labels(act_label).inc()

                ts_raw = s.get("ts")
                ts_ms: Optional[int]
                try:
                    ts_ms = int(ts_raw)
                except (TypeError, ValueError):
                    ts_ms = None
                if ts_ms is None:
                    ts_ms = int(time.time() * 1000)
                signal_lag = max(0.0, time.time() - (ts_ms / 1000.0))
                SIGNAL_LAG.observe(signal_lag)
                order_ts = ts_ms // 1000

                # require prices to size
                if act_label.startswith("ENTER") and (pxA <= 0 or pxB <= 0):
                    REJECTS.labels("no_px").inc()
                    await consumer.commit()
                    continue

                notional = max_per_trade
                if meta and meta.get("notional") is not None:
                    try:
                        notional = float(meta["notional"])
                    except Exception:
                        pass

                if act_label == "EXIT":
                    net = await positions_today(pool, pair_id)
                    qA, qB = abs(net["A"]), abs(net["B"])
                    if qA == 0 and qB == 0:
                        await consumer.commit()
                        continue
                    sideA = "SELL" if net["A"] > 0 else "BUY"
                    sideB = "SELL" if net["B"] > 0 else "BUY"
                    await T.allow(a, bucket)
                    await T.allow(b, bucket)
                    pair_symbol = meta.get("symbol") if meta else f"{a}-{b}"
                    extra_common = {
                        "pair_id": pair_id,
                        "pair_symbol": pair_symbol,
                        "avg_hold_min": avg_hold,
                        "risk_per_trade": risk_override,
                    }
                    oA = {
                        "client_order_id": coid(pair_id, "A"),
                        "symbol": a,
                        "side": sideA,
                        "qty": qA,
                        "order_type": "MKT",
                        "strategy": strategy_name,
                        "risk_bucket": bucket,
                        "status": "NEW",
                        "ts": order_ts,
                        "reason": "EXIT",
                        "extra": {**extra_common, "leg": "A", "reason": "EXIT"},
                    }
                    oB = {
                        "client_order_id": coid(pair_id, "B"),
                        "symbol": b,
                        "side": sideB,
                        "qty": qB,
                        "order_type": "MKT",
                        "strategy": strategy_name,
                        "risk_bucket": bucket,
                        "status": "NEW",
                        "ts": order_ts,
                        "reason": "EXIT",
                        "extra": {**extra_common, "leg": "B", "reason": "EXIT"},
                    }
                    await oms.upsert_new(oA)
                    await oms.upsert_new(oB)
                    await producer.send_and_wait(OUT_TOPIC, json.dumps(oA).encode(), key=a.encode())
                    await producer.send_and_wait(OUT_TOPIC, json.dumps(oB).encode(), key=b.encode())
                    ORDERS_EMIT.labels(pair_id, "A", bucket, "EXIT").inc()
                    ORDERS_EMIT.labels(pair_id, "B", bucket, "EXIT").inc()
                    positions_state.pop(pair_id, None)
                    save_state(positions_state)
                    OPEN_PAIRS.set(len(positions_state))
                    ORDER_PUBLISH_LAG.observe(max(0.0, time.time() - (ts_ms / 1000.0)))
                    exit_reason = (oA.get("extra") or {}).get("reason", "EXIT")
                    print(f"[pairs-exec] EXIT {pair_symbol} bucket={bucket} qA={qA} qB={qB} reason={exit_reason}")
                    await consumer.commit()
                    continue

                qA, qB = size_legs(notional, pxA, pxB, beta)
                if qA < 1 or qB < 1:
                    REJECTS.labels("tiny_qty").inc()
                    await consumer.commit()
                    continue

                if act_label == "ENTER_LONG_A_SHORT_B":
                    sideA, sideB = "BUY", "SELL"
                elif act_label == "ENTER_SHORT_A_LONG_B":
                    sideA, sideB = "SELL", "BUY"
                else:
                    REJECTS.labels("bad_action").inc()
                    await consumer.commit()
                    continue

                existing_state = positions_state.get(pair_id)
                if existing_state and existing_state.get("position"):
                    print(
                        f"[pairs-exec] SKIP entry for {pair_symbol} action={act_label}; tracked position={existing_state}"
                    )
                    await consumer.commit()
                    continue

                await T.allow(a, bucket)
                await T.allow(b, bucket)
                pair_symbol = meta.get("symbol") if meta else f"{a}-{b}"
                extra_common = {
                    "pair_id": pair_id,
                    "pair_symbol": pair_symbol,
                    "beta": beta,
                    "avg_hold_min": avg_hold,
                    "risk_per_trade": risk_override,
                }

                oA = {
                    "client_order_id": coid(pair_id, "A"),
                    "symbol": a,
                    "side": sideA,
                    "qty": qA,
                    "order_type": "MKT",
                    "strategy": strategy_name,
                    "risk_bucket": bucket,
                    "status": "NEW",
                    "ts": order_ts,
                    "reason": act_label,
                    "extra": {**extra_common, "leg": "A", "px_ref": pxA},
                }
                oB = {
                    "client_order_id": coid(pair_id, "B"),
                    "symbol": b,
                    "side": sideB,
                    "qty": qB,
                    "order_type": "MKT",
                    "strategy": strategy_name,
                    "risk_bucket": bucket,
                    "status": "NEW",
                    "ts": order_ts,
                    "reason": act_label,
                    "extra": {**extra_common, "leg": "B", "px_ref": pxB},
                }
                await oms.upsert_new(oA)
                await oms.upsert_new(oB)
                await producer.send_and_wait(OUT_TOPIC, json.dumps(oA).encode(), key=a.encode())
                await producer.send_and_wait(OUT_TOPIC, json.dumps(oB).encode(), key=b.encode())
                ORDERS_EMIT.labels(pair_id, "A", bucket, "ENTER").inc()
                ORDERS_EMIT.labels(pair_id, "B", bucket, "ENTER").inc()
                positions_state[pair_id] = {
                    "pair_symbol": pair_symbol,
                    "position": act_label,
                    "sideA": sideA,
                    "sideB": sideB,
                    "qtyA": qA,
                    "qtyB": qB,
                    "bucket": bucket,
                    "beta": beta,
                    "ts": order_ts,
                }
                save_state(positions_state)
                OPEN_PAIRS.set(len(positions_state))
                ORDER_PUBLISH_LAG.observe(max(0.0, time.time() - (ts_ms / 1000.0)))
                beta_log = (oA.get("extra") or {}).get("beta")
                print(
                    f"[pairs-exec] ENTER {pair_symbol} action={act_label} bucket={bucket} qA={qA} qB={qB} beta={beta_log}"
                )
                await consumer.commit()
            finally:
                PROCESS_TIME.labels(act_label).observe(max(0.0, time.perf_counter() - start_proc))
    finally:
        await consumer.stop(); await producer.stop(); await pool.close()

if __name__=="__main__":
    asyncio.run(main())
