"""
monitoring/pairs_exporter.py
Exports live pair metrics from Kafka topic `pairs.signals`.

Metrics
- pair_zscore{pair_id,a,b}     (last z per pair)
- pair_spread{pair_id}         (last spread if pxA/pxB present)
- pair_in_position{pair_id}    (-1 shortA/longB, 0 flat, +1 longA/shortB)
- pair_entries_total{pair_id}
- pair_exits_total{pair_id}

Run:
  KAFKA_BROKER=localhost:9092 METRICS_PORT=8020 python monitoring/pairs_exporter.py
"""
from __future__ import annotations
import os, asyncio, math, ujson as json
from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server, Gauge, Counter

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
TOPIC=os.getenv("IN_TOPIC","pairs.signals")
GROUP=os.getenv("GROUP_ID","pairs_exporter")
PORT=int(os.getenv("METRICS_PORT","8020"))

Z = Gauge("pair_zscore", "Latest z-score per pair", ["pair_id","a","b"])
S = Gauge("pair_spread", "Latest spread (logA - beta*logB)", ["pair_id"])
POS = Gauge("pair_in_position", "Position dir: -1 shortA/longB, 0 flat, +1 longA/shortB", ["pair_id"])
ENT = Counter("pair_entries_total", "Entries per pair", ["pair_id"])
EXT = Counter("pair_exits_total", "Exits per pair", ["pair_id"])

async def main():
    start_http_server(PORT)
    print(f"[pairs-exp] :{PORT}/metrics from {TOPIC}")
    consumer=AIOKafkaConsumer(
        TOPIC, bootstrap_servers=BROKER, group_id=GROUP,
        enable_auto_commit=True, auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None
    )
    await consumer.start()
    try:
        async for m in consumer:
            s=m.value
            pid=str(s["pair_id"]); a=s["a_symbol"]; b=s["b_symbol"]; beta=float(s["beta"])
            z=float(s.get("z",0.0)); Z.labels(pid,a,b).set(z)
            # spread if prices provided
            if "pxA" in s and "pxB" in s and s["pxA"]>0 and s["pxB"]>0:
                sp = math.log(float(s["pxA"])) - beta*math.log(float(s["pxB"]))
                S.labels(pid).set(sp)
            act=(s.get("action") or "")
            if act.startswith("ENTER_"):
                POS.labels(pid).set(+1 if "LONG_A_SHORT_B" in act else -1)
                ENT.labels(pid).inc()
            elif act.startswith("EXIT_"):
                POS.labels(pid).set(0)
                EXT.labels(pid).inc()
    finally:
        await consumer.stop()

if __name__=="__main__":
    asyncio.run(main())
