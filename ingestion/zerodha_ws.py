"""
Zerodha KiteTicker → Kafka 'ticks' topic.
- Reads tokens from configs/tokens.csv (col: instrument_token,tradingsymbol,subscribe=1/0)
- Loads access_token from ingestion/auth/token.json (created via your login flow).
- Uses a thread-safe Queue to bridge KiteTicker callbacks → asyncio Kafka producer.
- Validates shape; pushes minimal tick schema (symbol,event_ts,ltp,vol).

Run:
  KAFKA_BROKER=localhost:9092 python ingestion/zerodha_ws_v2.py
"""
import os, csv, json, time, queue, threading, asyncio, ujson as ujson
from datetime import datetime, timezone
from pathlib import Path

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from aiokafka import AIOKafkaProducer
from kiteconnect import KiteTicker, KiteConnect, exceptions as kz_ex
import yaml

TOKENS_CSV=os.getenv("ZERODHA_TOKENS_CSV","configs/tokens.csv")
PAIRS_YAML=os.getenv("PAIRS_NEXT_DAY","configs/pairs_next_day.yaml")
API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")
BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
TOPIC=os.getenv("TICKS_TOPIC","ticks")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8111"))

QUEUE_DEPTH = Gauge("zerodha_queue_depth", "Number of ticks buffered for Kafka dispatch")
TICKS_RECEIVED = Counter("zerodha_ticks_received_total", "Ticks forwarded to Kafka", ["symbol"])
TICKS_DROPPED = Counter("zerodha_ticks_dropped_total", "Ticks dropped due to full queue")
BATCH_SIZE = Histogram(
    "zerodha_tick_batch_size",
    "Batch size of ticks forwarded to Kafka",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500),
)
TICK_LATENCY = Histogram(
    "zerodha_tick_latency_seconds",
    "Latency between exchange timestamp and Kafka publish",
    buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)
WS_EVENTS = Counter("zerodha_ws_events_total", "Websocket lifecycle events", ["event"])

def load_pair_symbols(yaml_path: str) -> set:
    path = Path(yaml_path)
    if not path.exists():
        return set()
    try:
        doc = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return set()
    symbols = set()
    for row in doc.get("selections", []) or []:
        sym = str(row.get("symbol") or "")
        if "-" not in sym:
            continue
        leg_a, leg_b = [s.strip().upper() for s in sym.split("-", 1)]
        if leg_a:
            symbols.add(leg_a)
        if leg_b:
            symbols.add(leg_b)
    return symbols

def load_tokens():
    watch_symbols = load_pair_symbols(PAIRS_YAML)
    if not watch_symbols:
        print(f"[ws] warning: could not read {PAIRS_YAML}; subscribing to all tokens")
    out=[]; map_ts={}
    with open(TOKENS_CSV) as f:
        r=csv.DictReader(f)
        for row in r:
            if (row.get("subscribe","1") or "1").strip().lower() in ("1","y","yes","true"):
                sym=row.get("tradingsymbol") or ""
                sym_clean = sym.strip().upper()
                if watch_symbols and sym_clean not in watch_symbols:
                    continue
                tok=int(row["instrument_token"]); sym_final=sym or str(tok)
                out.append(tok); map_ts[tok]=sym_final
    if not out: raise SystemExit("No tokens in configs/tokens.csv")
    return out, map_ts

def load_access_token():
    T=json.load(open(TOKEN_FILE))
    if not T.get("access_token"): raise SystemExit("access_token missing; run login")
    if API_KEY and T.get("api_key") and T["api_key"] != API_KEY:
        raise SystemExit("api_key mismatch; re-auth or set KITE_API_KEY to match")
    return T.get("api_key") or API_KEY, T["access_token"]

async def drain_to_kafka(q:queue.Queue, symmap:dict):
    prod=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await prod.start()
    try:
        while True:
            batch=[]
            try:
                # batch up to 500 msgs or 250 ms
                for _ in range(500):
                    batch.append(q.get(timeout=0.25))
                    QUEUE_DEPTH.set(q.qsize())
            except queue.Empty:
                pass
            size = len(batch)
            if size == 0:
                continue
            BATCH_SIZE.observe(size)
            for t in batch:
                tok = t["instrument_token"]; sym=symmap.get(tok, str(tok))
                ltp = float(t.get("last_price") or t.get("ltp") or 0.0)
                vol = int(t.get("volume_traded") or 0)
                # ns epoch preferred; fallback millis
                et = t.get("exchange_timestamp") or t.get("timestamp")
                if isinstance(et, datetime):
                    ns = int(et.replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)
                else:
                    ns = int(time.time()*1_000_000_000)
                event_latency = max(0.0, (time.time_ns() - ns) / 1_000_000_000.0)
                TICK_LATENCY.observe(event_latency)
                TICKS_RECEIVED.labels(sym).inc()
                msg={"symbol":sym,"event_ts":ns,"ltp":ltp,"vol":vol}
                await prod.send_and_wait(TOPIC, ujson.dumps(msg).encode(), key=sym.encode())
    finally:
        await prod.stop()

def main():
    start_http_server(METRICS_PORT)
    tokens, symmap = load_tokens()
    api_key, access = load_access_token()
    # quick REST check
    kc=KiteConnect(api_key=api_key); kc.set_access_token(access)
    try: kc.profile()
    except kz_ex.TokenException: raise SystemExit("Token invalid/expired. Re-auth.")

    q=queue.Queue(maxsize=10000)
    QUEUE_DEPTH.set(0)
    kt=KiteTicker(api_key, access)
    def on_ticks(ws, ticks):
        WS_EVENTS.labels("ticks").inc()
        for t in ticks:
            try: q.put_nowait(t)
            except queue.Full:
                TICKS_DROPPED.inc()
            finally:
                QUEUE_DEPTH.set(q.qsize())
    def on_connect(ws, response):
        ws.subscribe(tokens); ws.set_mode(ws.MODE_LTP, tokens)
        print(f"[ws] connected; subscribed {len(tokens)}")
        WS_EVENTS.labels("connect").inc()
    def on_error(ws, code, reason): print(f"[ws] error {code}: {reason}")
    def on_close(ws, code, reason):
        print(f"[ws] closed {code}: {reason}")
        WS_EVENTS.labels("close").inc()

    kt.on_ticks = on_ticks; kt.on_connect = on_connect
    kt.on_error = on_error; kt.on_close = on_close

    t = threading.Thread(target=lambda: kt.connect(threaded=False), daemon=True)
    t.start()
    try:
        asyncio.run(drain_to_kafka(q, symmap))
    finally:
        try: kt.close()
        except: pass

if __name__=="__main__":
    main()
