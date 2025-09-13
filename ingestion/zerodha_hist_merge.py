"""
Fetch 1m candles from Zerodha for symbols in tokens.csv and upsert into bars_1m.
Usage:
  D1=2025-06-01 D2=2025-09-01 python ingestion/zerodha_hist_merge.py
Env:
  KITE_API_KEY, ZERODHA_TOKEN_FILE, POSTGRES_* (host,port,db,user,password)
"""
import os, csv, json, asyncio, asyncpg
from datetime import datetime, timezone
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv(".env"); load_dotenv("infra/.env")

TOKENS_CSV=os.getenv("ZERODHA_TOKENS_CSV","configs/tokens.csv")
API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

D1=os.getenv("D1"); D2=os.getenv("D2")
if not (D1 and D2): raise SystemExit("Set D1/D2 as YYYY-MM-DD")

UPSERT = """
INSERT INTO bars_1m(symbol,ts,o,h,l,c,vol,n_trades)
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT(symbol,ts) DO UPDATE SET
h=GREATEST(bars_1m.h, EXCLUDED.h),
l=LEAST(bars_1m.l, EXCLUDED.l),
c=EXCLUDED.c,
vol=bars_1m.vol + EXCLUDED.vol,
n_trades=bars_1m.n_trades + EXCLUDED.n_trades;
"""

def load_symbols():
    out=[]
    with open(TOKENS_CSV) as f:
        r=csv.DictReader(f)
        for row in r:
            if (row.get("subscribe","1") or "1").strip().lower() in ("1","y","true","yes"):
                out.append((int(row["instrument_token"]), row["tradingsymbol"]))
    return out

async def main():
    tok=json.load(open(TOKEN_FILE))["access_token"]
    k=KiteConnect(api_key=API_KEY); k.set_access_token(tok)
    syms=load_symbols()
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    try:
        for itoken, sym in syms:
            candles = k.historical_data(itoken, D1, D2, "minute", continuous=False, oi=False)
            rows=[]
            for c in candles:
                ts = c["date"].astimezone(timezone.utc).replace(second=0, microsecond=0)
                rows.append((sym, ts, float(c["open"]), float(c["high"]), float(c["low"]),
                             float(c["close"]), int(c["volume"]), int(c.get("trades") or 1)))
            async with pool.acquire() as con:
                async with con.transaction():
                    await con.executemany(UPSERT, rows)
            print(f"[hist] {sym}: upserted {len(rows)} bars")
    finally:
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
