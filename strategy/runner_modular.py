# strategy/runner_modular.py
import os, asyncio, ujson as json
from collections import defaultdict, deque
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import yaml
from libs.signal import Signal, NONE
from strategy.sma_cross import SMA_Cross
from strategy.rsi_reversion import RSI_Reversion
from indicators.sma import SMA
from indicators.rsi import RSI
from indicators.atr import ATR
from libs.killswitch import KillSwitch

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
IN_TOPIC=os.getenv("IN_TOPIC","bars.1m")      # run per TF instance
OUT_TOPIC=os.getenv("OUT_TOPIC","orders")
GROUP_ID=os.getenv("KAFKA_GROUP","runner_modular_1m")
TF=os.getenv("TF","1m")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

CONF_STRATS=os.getenv("STRAT_CONF","strategy/registry.yaml")
CONF_ENS=os.getenv("ENS_CONF","strategy/ensemble.yaml")
CONF_INDS=os.getenv("IND_CONF","configs/indicators.yaml")

KS=KillSwitch()

def load_indicators(conf, tf):
    kinds = {"SMA": SMA, "RSI": RSI, "ATR": ATR}
    out=[]
    for spec in conf["timeframes"].get(tf, []):
        cls = kinds[spec["kind"]]
        out.append(cls(**(spec.get("params") or {})))
    return out

def load_strategies(conf, tf):
    kinds = {"SMA_Cross": SMA_Cross, "RSI_Reversion": RSI_Reversion}
    out=[]
    for s in conf["strategies"]:
        if tf not in (s.get("timeframes") or []): continue
        out.append( kinds[s["name"]](**(s.get("params") or {})) )
    return out

def load_ensemble(conf):
    from strategy.ensemble_engine import EnsembleEngine
    return EnsembleEngine(mode=conf.get("mode","majority"),
                          k=int(conf.get("k",1)),
                          weights=conf.get("weights") or {},
                          min_score=float(conf.get("min_score",0.0)))

async def persist_order(pool, order):
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO orders(client_order_id, ts, symbol, side, qty, order_type, strategy, reason, risk_bucket, status, extra) "
            "VALUES($1,to_timestamp($2),$3,$4,$5,$6,$7,$8,$9,$10,$11) "
            "ON CONFLICT (client_order_id) DO NOTHING",
            order["client_order_id"], order["ts"], order["symbol"], order["side"],
            order["qty"], order["order_type"], order["strategy"], order["reason"],
            order["risk_bucket"], order["status"], json.dumps(order.get("extra",{}))
        )

def mk_coid(stname, sym, ts): return f"{stname}:{sym}:{ts}"

async def main():
    inds_conf = yaml.safe_load(open(CONF_INDS))
    strat_conf = yaml.safe_load(open(CONF_STRATS))
    ens_conf = yaml.safe_load(open(CONF_ENS))

    indicators = load_indicators(inds_conf, TF)
    strategies = load_strategies(strat_conf, TF)
    ensemble = load_ensemble(ens_conf)

    # rolling buffers
    closes=defaultdict(lambda: deque(maxlen=300))
    hlc=defaultdict(lambda: deque(maxlen=300))  # (o,h,l,c)

    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    cons=AIOKafkaConsumer(IN_TOPIC, bootstrap_servers=BROKER, enable_auto_commit=False,
                          auto_offset_reset="earliest", group_id=GROUP_ID,
                          value_deserializer=lambda b: json.loads(b.decode()),
                          key_deserializer=lambda b: b.decode() if b else None)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await cons.start(); await prod.start()
    try:
        while True:
            if KS.is_tripped()[0]:
                print("[KILL] stopping runner"); break

            batches=await cons.getmany(timeout_ms=1000, max_records=2000)
            for _tp, msgs in batches.items():
                for m in msgs:
                    r=m.value; sym=r["symbol"]; ts=int(r["ts"])
                    o,h,l,c=float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"])
                    v=int(r.get("vol") or 0)

                    closes[sym].append(c)
                    hlc[sym].append((o,h,l,c))

                    # build features
                    feat = {"ready": True}
                    for ind in indicators:
                        if isinstance(ind, ATR):
                            res = ind.update(hlc[sym])
                        else:
                            res = ind.update(closes[sym])
                        feat["ready"] = feat["ready"] and bool(res.get("ready"))
                        feat.update({k:v for k,v in res.items() if k!="ready"})

                    # run strategies
                    sigs=[]
                    for s in strategies:
                        sig = s.on_bar(sym, TF, ts, o,h,l,c,v, feat)
                        sigs.append((s.name, sig))

                    # ensemble
                    ens_sig = ensemble.combine(sigs)
                    if ens_sig.side in ("NONE",):
                        continue

                    # order skeleton (qty=1; risk sizes later)
                    order = {
                        "client_order_id": mk_coid("ENS", sym, ts),
                        "ts": ts,
                        "symbol": sym,
                        "side": "BUY" if ens_sig.side=="BUY" else ("SELL" if ens_sig.side=="SELL" else "EXIT"),
                        "qty": 1,
                        "order_type": "MKT",
                        "strategy": "Ensemble",
                        "reason": f"{ens_sig.tags}",
                        "risk_bucket": "MED",
                        "status": "NEW",
                        "extra": {"signal": ens_sig.to_dict()}
                    }
                    await persist_order(pool, order)
                    await prod.send_and_wait("orders", json.dumps(order).encode(), key=sym.encode())
            await cons.commit()
    finally:
        await cons.stop(); await prod.stop(); await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
