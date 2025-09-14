"""
ingestion/nfo_instruments.py
Fetch Zerodha instruments for NFO (FUT/OPT), persist to Postgres + CSV.

Tables auto-created:
  instruments_nfo(instrument_token, tradingsymbol, name, segment, exchange,
                  instrument_type, strike, expiry, lot_size, tick_size)

ENV: POSTGRES_*, KITE_API_KEY, ZERODHA_TOKEN_FILE
"""
from __future__ import annotations
import os, csv, json, asyncpg
from datetime import datetime
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv(".env"); load_dotenv("infra/.env")
PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
KITE_API_KEY=os.getenv("KITE_API_KEY"); TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

DDL = """
CREATE TABLE IF NOT EXISTS instruments_nfo(
  instrument_token bigint PRIMARY KEY,
  tradingsymbol text NOT NULL,
  name text,
  segment text,
  exchange text,
  instrument_type text,
  strike double precision,
  expiry date,
  lot_size int,
  tick_size double precision
);
"""

def _access():
    t=json.load(open(TOKEN_FILE))
    if t.get("api_key")!=KITE_API_KEY: raise SystemExit("Token API key mismatch")
    return t["access_token"]

async def main():
    access=_access(); kite=KiteConnect(api_key=KITE_API_KEY); kite.set_access_token(access)
    ins = kite.instruments("NFO")  # list of dicts
    rows=[]
    for r in ins:
        itype=r.get("instrument_type") or ""
        if itype not in ("FUT","CE","PE"): continue
        rows.append({
            "instrument_token": int(r["instrument_token"]),
            "tradingsymbol": r["tradingsymbol"],
            "name": r.get("name"),
            "segment": r.get("segment"),
            "exchange": r.get("exchange"),
            "instrument_type": itype,
            "strike": float(r.get("strike") or 0.0),
            "expiry": (r.get("expiry") or "").split("T")[0] or None,
            "lot_size": int(r.get("lot_size") or 0),
            "tick_size": float(r.get("tick_size") or 0.05),
        })
    # write CSV snapshot
    os.makedirs("data/instruments", exist_ok=True)
    snap=f"data/instruments/instruments_NFO_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    with open(snap,"w",newline="") as f:
        w=csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    print("[nfo] snapshot:", snap, "rows:", len(rows))
    # upsert
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        async with pool.acquire() as con:
            await con.execute(DDL)
            await con.executemany("""
              INSERT INTO instruments_nfo(instrument_token,tradingsymbol,name,segment,exchange,instrument_type,strike,expiry,lot_size,tick_size)
              VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
              ON CONFLICT (instrument_token) DO UPDATE
              SET tradingsymbol=EXCLUDED.tradingsymbol, name=EXCLUDED.name, segment=EXCLUDED.segment,
                  exchange=EXCLUDED.exchange, instrument_type=EXCLUDED.instrument_type, strike=EXCLUDED.strike,
                  expiry=EXCLUDED.expiry, lot_size=EXCLUDED.lot_size, tick_size=EXCLUDED.tick_size
            """, [(r["instrument_token"], r["tradingsymbol"], r["name"], r["segment"], r["exchange"],
                   r["instrument_type"], r["strike"], r["expiry"], r["lot_size"], r["tick_size"]) for r in rows])
    finally:
        await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
