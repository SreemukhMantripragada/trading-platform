"""
Compare local bars_1m with Zerodha historical for a day (IST market window).
Writes recon_runs/summary/detail (same schema as earlier).
Usage:
  python monitoring/daily_recon.py --date 2025-09-09 --symbols RELIANCE,TCS
"""
import os, csv, json, argparse, asyncio, asyncpg
from datetime import datetime, time, timedelta, timezone
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv(".env"); load_dotenv("infra/.env")
API_KEY=os.getenv("KITE_API_KEY"); TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")
TOKENS_CSV=os.getenv("ZERODHA_TOKENS_CSV","configs/tokens.csv")
PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
TOL_PX_ABS=float(os.getenv("RECON_TOL_PX_ABS","0.02")); TOL_PX_REL=float(os.getenv("RECON_TOL_PX_REL","0.001")); TOL_VOL=int(os.getenv("RECON_TOL_VOL_ABS","5"))

def ist_window(date_yyyy_mm_dd:str):
    d=datetime.fromisoformat(date_yyyy_mm_dd)
    ist=timezone(timedelta(hours=5,minutes=30))
    s=datetime.combine(d.date(), time(9,15), ist).astimezone(timezone.utc)
    e=datetime.combine(d.date(), time(15,30), ist).astimezone(timezone.utc)
    return s,e

def load_map():
    m={}
    with open(TOKENS_CSV) as f:
        for r in csv.DictReader(f):
            m[(r["tradingsymbol"]).strip()] = int(r["instrument_token"])
    return m

async def ensure_schema(pool):
    sql="""
    CREATE TABLE IF NOT EXISTS recon_runs(
      run_id bigserial PRIMARY KEY, run_date date NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
      window_start timestamptz NOT NULL, window_end timestamptz NOT NULL,
      tol_px_abs double precision NOT NULL, tol_px_rel double precision NOT NULL, tol_vol_abs integer NOT NULL);
    CREATE TABLE IF NOT EXISTS recon_summary(
      run_id bigint REFERENCES recon_runs(run_id) ON DELETE CASCADE, symbol text NOT NULL,
      n_bars int NOT NULL, n_miss_local int NOT NULL, n_miss_vendor int NOT NULL,
      n_ohlc_mismatch int NOT NULL, n_vol_mismatch int NOT NULL);
    CREATE TABLE IF NOT EXISTS recon_detail(
      run_id bigint REFERENCES recon_runs(run_id) ON DELETE CASCADE, symbol text NOT NULL, ts timestamptz NOT NULL,
      local_o double precision, local_h double precision, local_l double precision, local_c double precision, local_v int,
      vendor_o double precision, vendor_h double precision, vendor_l double precision, vendor_c double precision, vendor_v int,
      reason text);
    """
    async with pool.acquire() as con: await con.execute(sql)

def within(local, vendor):
    if vendor==0: return local==0
    return abs(local - vendor) <= max(TOL_PX_ABS, TOL_PX_REL*vendor)

async def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--date", required=True); ap.add_argument("--symbols", default="")
    args=ap.parse_args()
    s,e=ist_window(args.date)
    sym_filter=set([x.strip() for x in args.symbols.split(",") if x.strip()]) if args.symbols else None

    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS); await ensure_schema(pool)
    tok=json.load(open(TOKEN_FILE))["access_token"]; k=KiteConnect(api_key=API_KEY); k.set_access_token(tok)
    sym2tok=load_map()

    async with pool.acquire() as con:
        r=await con.fetchrow("INSERT INTO recon_runs(run_date,window_start,window_end,tol_px_abs,tol_px_rel,tol_vol_abs) VALUES($1,$2,$3,$4,$5,$6) RETURNING run_id",
                             s.date(), s, e, TOL_PX_ABS, TOL_PX_REL, TOL_VOL)
        run_id=int(r["run_id"])

    for sym, tokid in sym2tok.items():
        if sym_filter and sym not in sym_filter: continue
        vendor=k.historical_data(tokid, s, e, "minute", continuous=False, oi=False)
        vmap={}
        for c in vendor:
            ts=c["date"].astimezone(timezone.utc).replace(second=0,microsecond=0)
            vmap[ts]=(float(c["open"]),float(c["high"]),float(c["low"]),float(c["close"]),int(c["volume"]))
        async with pool.acquire() as con:
            rows=await con.fetch("SELECT ts,o,h,l,c,vol FROM bars_1m WHERE symbol=$1 AND ts BETWEEN $2 AND $3 ORDER BY ts", sym, s, e)
        lmap={ r["ts"].astimezone(timezone.utc).replace(second=0,microsecond=0):(float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"]),int(r["vol"])) for r in rows }

        keys=sorted(set(lmap.keys())|set(vmap.keys()))
        miss_l=miss_v=ohlc=volm=0; details=[]
        for ts in keys:
            L=lmap.get(ts); V=vmap.get(ts)
            if L is None: miss_l+=1; details.append((ts,None,V,"missing_local")); continue
            if V is None: miss_v+=1; details.append((ts,L,None,"missing_vendor")); continue
            reasons=[]
            if not (within(L[0],V[0]) and within(L[1],V[1]) and within(L[2],V[2]) and within(L[3],V[3])): reasons.append("ohlc"); ohlc+=1
            if abs(L[4]-V[4]) > TOL_VOL: reasons.append("vol"); volm+=1
            if reasons: details.append((ts,L,V,",".join(reasons)))
        async with pool.acquire() as con:
            await con.execute("INSERT INTO recon_summary(run_id,symbol,n_bars,n_miss_local,n_miss_vendor,n_ohlc_mismatch,n_vol_mismatch) VALUES($1,$2,$3,$4,$5,$6,$7)",
                              run_id, sym, len(keys), miss_l, miss_v, ohlc, volm)
            if details:
                await con.executemany(
                  "INSERT INTO recon_detail(run_id,symbol,ts,local_o,local_h,local_l,local_c,local_v,vendor_o,vendor_h,vendor_l,vendor_c,vendor_v,reason) "
                  "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                  [(run_id,sym,ts,
                    (None if L is None else L[0]),(None if L is None else L[1]),(None if L is None else L[2]),(None if L is None else L[3]),(None if L is None else L[4]),
                    (None if V is None else V[0]),(None if V is None else V[1]),(None if V is None else V[2]),(None if V is None else V[3]),(None if V is None else V[4]),
                    reason) for (ts,L,V,reason) in details ])
    print(f"✅ recon_v2 done run_id={run_id} window={s}→{e}")

if __name__=="__main__":
    asyncio.run(main())
