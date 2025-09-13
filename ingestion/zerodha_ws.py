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
from aiokafka import AIOKafkaProducer
from kiteconnect import KiteTicker, KiteConnect, exceptions as kz_ex

TOKENS_CSV=os.getenv("ZERODHA_TOKENS_CSV","configs/tokens.csv")
API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")
BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
TOPIC=os.getenv("TICKS_TOPIC","ticks")

def load_tokens():
    out=[]; map_ts={}
    with open(TOKENS_CSV) as f:
        r=csv.DictReader(f)
        for row in r:
            if (row.get("subscribe","1") or "1").strip().lower() in ("1","y","yes","true"):
                tok=int(row["instrument_token"]); sym=row.get("tradingsymbol") or str(tok)
                out.append(tok); map_ts[tok]=sym
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
            except queue.Empty:
                pass
            if not batch: continue
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
                msg={"symbol":sym,"event_ts":ns,"ltp":ltp,"vol":vol}
                await prod.send_and_wait(TOPIC, ujson.dumps(msg).encode(), key=sym.encode())
    finally:
        await prod.stop()

def main():
    tokens, symmap = load_tokens()
    api_key, access = load_access_token()
    # quick REST check
    kc=KiteConnect(api_key=api_key); kc.set_access_token(access)
    try: kc.profile()
    except kz_ex.TokenException: raise SystemExit("Token invalid/expired. Re-auth.")

    q=queue.Queue(maxsize=10000)
    kt=KiteTicker(api_key, access)
    def on_ticks(ws, ticks):
        for t in ticks:
            try: q.put_nowait(t)
            except queue.Full: pass
    def on_connect(ws, response):
        ws.subscribe(tokens); ws.set_mode(ws.MODE_LTP, tokens)
        print(f"[ws] connected; subscribed {len(tokens)}")
    def on_error(ws, code, reason): print(f"[ws] error {code}: {reason}")
    def on_close(ws, code, reason): print(f"[ws] closed {code}: {reason}")

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
