"""
ingestion/merge_hist_daily.py
Fetches yesterday's 1m bars from Zerodha for symbols in configs/tokens.csv
and UPSERTs into bars_1m. Safe to run daily (idempotent per (symbol, ts)).

ENV: KITE_API_KEY, ZERODHA_TOKEN_FILE, POSTGRES_*.
Run:
  python ingestion/merge_hist_daily.py
"""
from __future__ import annotations
import os, csv, asyncpg
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from kiteconnect import KiteConnect, exceptions as kz

load_dotenv(".env"); load_dotenv("infra/.env")
KITE_API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

TOKENS_CSV=os.getenv("ZERODHA_TOKENS_CSV","configs/tokens.csv")

UPSERT = """
INSERT INTO bars_1m(symbol, ts, o, h, l, c, vol, n_trades)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (symbol, ts) DO UPDATE
SET o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c, vol=EXCLUDED.vol, n_trades=EXCLUDED.n_trades;
"""

def load_access():
    import json
    t=json.load(open(TOKEN_FILE))
    if t.get("api_key")!=KITE_API_KEY: raise SystemExit("Token API key mismatch")
    return t["access_token"]

def load_symbols():
    out=[]
    with open(TOKENS_CSV, newline="") as f:
        r=csv.DictReader(f)
        for row in r:
            if (row.get("subscribe","1") or "1").strip() not in ("0","false","no","False"):
                out.append((int(row["instrument_token"]), row["tradingsymbol"].strip()))
    if not out: raise SystemExit("No symbols in tokens.csv")
    return out

def ist_day_bounds_utc(d):
    ist = timezone(timedelta(hours=5, minutes=30))
    start_ist = datetime(d.year,d.month,d.day,9,15,tzinfo=ist)
    end_ist   = datetime(d.year,d.month,d.day,15,30,tzinfo=ist)
    return start_ist.astimezone(timezone.utc), end_ist.astimezone(timezone.utc)

async def main():
    from datetime import date
    access=load_access(); kite=KiteConnect(api_key=KITE_API_KEY); kite.set_access_token(access)
    try: kite.profile()
    except kz.TokenException: raise SystemExit("Zerodha token expired")

    syms=load_symbols()
    y=date.today() - timedelta(days=1)
    start_utc, end_utc = ist_day_bounds_utc(y)

    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        async with pool.acquire() as con:
            async with con.transaction():
                for tok, sym in syms:
                    candles=kite.historical_data(tok, start_utc, end_utc, "minute", continuous=False, oi=False)
                    batch=[]
                    for c in candles:
                        ts=c["date"].astimezone(timezone.utc).replace(second=0, microsecond=0)
                        batch.append((sym, ts, float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]), int(c["volume"]), 0))
                    if batch:
                        await con.executemany(UPSERT, batch)
        print(f"[merge_hist] merged {len(syms)} symbols for {y}")
    finally:
        await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
