"""
Recon auto-heal for minute bars (daily):
- For each subscribed symbol in configs/tokens.csv, fetch vendor minute bars for DATE.
- If counts or checksum mismatch, delete local day and insert vendor bars.
- Default DRY_RUN=1; pass APPLY=1 to actually write.

Env:
  DATE=2025-09-10 (optional; defaults to today IST)
  APPLY=0|1
  KITE_API_KEY / ZERODHA_TOKEN_FILE
  POSTGRES_HOST/PORT/DB/USER/PASSWORD
"""
from __future__ import annotations
import os, csv, asyncio, asyncpg, json as pyjson
from datetime import datetime, date, time, timedelta, timezone
from dotenv import load_dotenv
from kiteconnect import KiteConnect, exceptions as kz_ex

load_dotenv(".env"); load_dotenv("infra/.env")

KITE_API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

TOKENS_CSV=os.getenv("ZERODHA_TOKENS_CSV","configs/tokens.csv")
APPLY=(os.getenv("APPLY","0")=="1")
DATE_STR=os.getenv("DATE","")

def ist_window_for(d: date):
    ist=timezone(timedelta(hours=5,minutes=30))
    start=datetime.combine(d, time(9,15), ist).astimezone(timezone.utc)
    end  =datetime.combine(d, time(15,30), ist).astimezone(timezone.utc)
    return start, end

def _load_access_token()->str:
    t=pyjson.load(open(TOKEN_FILE))
    if t.get("api_key")!=KITE_API_KEY: raise SystemExit("Token/API key mismatch")
    return t["access_token"]

def load_symbols():
    out=[]
    with open(TOKENS_CSV, newline="") as f:
        r=csv.DictReader(f)
        for row in r:
            if (row.get("subscribe","1") or "1").strip().lower() not in ("1","true","y","yes"): continue
            out.append((int(row["instrument_token"]), row["tradingsymbol"].strip()))
    return out

async def fetch_local(pool, sym, start, end):
    sql="SELECT ts,o,h,l,c,vol FROM bars_1m WHERE symbol=$1 AND ts>=$2 AND ts<=$3 ORDER BY ts"
    async with pool.acquire() as con:
        rows=await con.fetch(sql, sym, start, end)
    return [{"ts": r["ts"].replace(second=0, microsecond=0), "o":float(r["o"]), "h":float(r["h"]),
             "l":float(r["l"]), "c":float(r["c"]), "vol":int(r["vol"])} for r in rows]

def checksum(rows):
    # simple checksum to detect diffs (count + sum close + sum vol)
    n=len(rows)
    s=sum(r["c"] for r in rows)
    v=sum(r["vol"] for r in rows)
    return (n, round(s,4), v)

async def upsert_day(pool, sym, vendor_rows):
    async with pool.acquire() as con:
        async with con.transaction():
            await con.execute("DELETE FROM bars_1m WHERE symbol=$1 AND ts::date=$2", sym, vendor_rows[0]["ts"].date())
            await con.executemany(
                "INSERT INTO bars_1m(symbol, ts, o,h,l,c,vol,n_trades) VALUES($1,to_timestamp($2),$3,$4,$5,$6,$7,$8) "
                "ON CONFLICT (symbol, ts) DO UPDATE SET o=EXCLUDED.o,h=EXCLUDED.h,l=EXCLUDED.l,c=EXCLUDED.c,vol=EXCLUDED.vol,n_trades=EXCLUDED.n_trades",
                [(sym, int(r["ts"].timestamp()), r["o"],r["h"],r["l"],r["c"],r["vol"], 1) for r in vendor_rows]
            )

async def main():
    # date
    ist=timezone(timedelta(hours=5,minutes=30))
    if DATE_STR:
        d=datetime.strptime(DATE_STR, "%Y-%m-%d").date()
    else:
        d=datetime.now(ist).date()
    start,end=ist_window_for(d)

    # kite
    access=_load_access_token()
    kite=KiteConnect(api_key=KITE_API_KEY); kite.set_access_token(access)
    try: kite.profile()
    except kz_ex.TokenException: raise SystemExit("Token invalid/expired")

    syms=load_symbols()
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)

    report=[]
    try:
        for tok, sym in syms:
            local=await fetch_local(pool, sym, start, end)
            vendor=kite.historical_data(tok, start, end, "minute", continuous=False, oi=False)
            vrows=[{"ts": c["date"].replace(second=0, microsecond=0), "o":float(c["open"]), "h":float(c["high"]),
                    "l":float(c["low"]), "c":float(c["close"]), "vol":int(c["volume"])} for c in vendor]
            c_loc, c_vnd = checksum(local), checksum(vrows)
            action="OK"
            if c_loc != c_vnd:
                action=("APPLY" if APPLY else "DRY")
                if APPLY and vrows:
                    await upsert_day(pool, sym, vrows)
            report.append({"symbol": sym, "local": c_loc, "vendor": c_vnd, "action": action})
        # print summary
        bad=[r for r in report if r["action"]!="OK"]
        print(f"[autoheal] {len(bad)}/ {len(report)} symbols changed ({'applied' if APPLY else 'dry-run'}) for {d}")
        for r in bad[:20]:
            print(f" - {r['symbol']}: local={r['local']} vendor={r['vendor']} -> {r['action']}")
    finally:
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
