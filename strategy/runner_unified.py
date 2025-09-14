"""
strategy/runner_unified.py
Unified signal→sizing→order emitter:
- IN: bars.1m (configurable)
- Load strategies from configs/strategies.yaml (module+class)
- For each new bar per symbol: strategy.decide(...) -> action
- Size with risk/position_sizer.size_order(...)
- OUT: orders (with px_ref) -> budget_guard -> orders.allowed -> gateway

ENV:
  KAFKA_BROKER, IN_TOPIC=bars.1m, OUT_TOPIC=orders, METRICS_PORT=8011
"""
from __future__ import annotations
import os, asyncio, ujson as json, importlib, time
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server, Counter, Gauge
from dotenv import load_dotenv
from strategy.base import Bar, Context
from risk.position_sizer import size_order

load_dotenv(".env"); load_dotenv("infra/.env")

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","bars.1m")
OUT_TOPIC=os.getenv("OUT_TOPIC","orders")
GROUP_ID=os.getenv("KAFKA_GROUP","runner_unified")
METRICS_PORT=int(os.getenv("METRICS_PORT","8011"))

ORDERS = Counter("runner_orders_total","orders emitted",["strategy","bucket","action"])
INTENTS = Counter("runner_intents_total","signals emitted",["strategy","action"])
LAT = Gauge("runner_decision_latency_ms","decision latency (ms)")
ERRS= Counter("runner_errors_total","errors",["where"])

def _load_config()->Dict[str,Any]:
    import yaml
    return yaml.safe_load(open("configs/strategies.yaml"))

def _mk_instance(spec:Dict[str,Any]):
    """
    spec:
      name: SMA_CROSS
      module: strategy.strategies.sma
      class: SMACross
      params: {fast: 10, slow: 20, bucket: MED}
    """
    mod=importlib.import_module(spec["module"])
    cls=getattr(mod, spec["class"])
    inst=cls(**(spec.get("params") or {}))
    inst.name=spec.get("name", inst.__class__.__name__)
    return inst

async def main():
    cfg=_load_config()
    syms=cfg.get("symbols") or ["RELIANCE","TCS"]
    strats=[_mk_instance(s) for s in (cfg.get("strategies") or [])]

    ctx=Context()
    start_http_server(METRICS_PORT)

    consumer=AIOKafkaConsumer(
        IN_TOPIC, bootstrap_servers=BROKER, group_id=GROUP_ID,
        enable_auto_commit=False, auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    producer=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await consumer.start(); await producer.start()
    print(f"[runner] {IN_TOPIC} -> {OUT_TOPIC}; symbols={len(syms)}, strats={[s.name for s in strats]}")
    try:
        async for m in consumer:
            try:
                r=m.value
                symbol=r["symbol"]; 
                if symbol not in syms: # restrict universe
                    await consumer.commit(); continue
                bar=Bar(ts=int(r["ts"]), o=float(r["o"]), h=float(r["h"]), l=float(r["l"]), c=float(r["c"]), vol=int(r.get("vol") or 0), n=int(r.get("n_trades") or 0))
                ctx.push_bar(symbol, bar)

                t0=time.time()
                for S in strats:
                    sig=None
                    try:
                        sig=S.decide(symbol, bar, ctx)
                    except Exception as e:
                        ERRS.labels("strategy").inc(); continue
                    if not sig: continue
                    INTENTS.labels(S.name, sig["action"]).inc()

                    # choose risk bucket & stop if provided
                    bucket=(sig.get("risk_bucket") or getattr(S, "default_bucket","MED")).upper()
                    stop_px=sig.get("stop_px")

                    # size order
                    qty, notional, meta = await size_order(
                        symbol=symbol, side=sig["action"], entry_px=bar.c, stop_px=stop_px,
                        bucket=bucket, product=("MIS" if time.gmtime(bar.ts).tm_hour < 9+5 else "CNC"),
                        risk_pct_per_trade=float(cfg.get("risk_pct_per_trade", 0.01)),
                        risk_abs_inr=None
                    )
                    if qty <= 0:
                        continue

                    coid=f"{int(bar.ts)}-{symbol}-{S.name}-{sig['action']}"
                    order={
                        "client_order_id": coid,
                        "symbol": symbol,
                        "side": sig["action"],
                        "qty": qty,
                        "price": 0.0,                # market; gateway may translate to LIMIT if needed
                        "strategy": S.name,
                        "risk_bucket": bucket,
                        "created_at": int(time.time()),
                        "extra": {
                            "px_ref": bar.c,
                            "stop_px": stop_px,
                            "sizer_meta": meta,
                            "reason": sig.get("reason","")
                        }
                    }
                    await producer.send_and_wait(OUT_TOPIC, json.dumps(order).encode(), key=symbol.encode())
                    ORDERS.labels(S.name, bucket, sig["action"]).inc()
                LAT.set((time.time()-t0)*1000.0)
                await consumer.commit()
            except Exception:
                ERRS.labels("loop").inc()
    finally:
        await consumer.stop(); await producer.stop()

if __name__=="__main__":
    asyncio.run(main())
