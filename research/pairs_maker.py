"""
Discover candidate pairs by correlation & (optional) half-life proxy over last N days (1m closes).
Usage: python research/pairs_maker.py --symbols RELIANCE,TCS,INFY --days 60
"""
import os, argparse, itertools, asyncpg, numpy as np, pandas as pd
from datetime import datetime, timedelta, timezone

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

async def fetch_closes(pool, sym, days):
    start = datetime.now(timezone.utc) - timedelta(days=days)
    async with pool.acquire() as con:
        rows=await con.fetch("SELECT ts,c FROM bars_1m WHERE symbol=$1 AND ts >= $2 ORDER BY ts", sym, start)
    return pd.Series([float(r["c"]) for r in rows])

def half_life(series):
    # crude proxy: time to mean-revert 50% based on AR(1) phi; avoid overfit in demo
    if len(series)<10: return None
    x=np.array(series[:-1]); y=np.array(series[1:])
    phi=(x*y).sum()/(x*x).sum() if (x*x).sum()!=0 else 0.0
    if phi<=0 or phi>=1: return None
    return np.log(0.5)/np.log(phi)

async def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--symbols", required=True); ap.add_argument("--days", type=int, default=60)
    a=ap.parse_args(); syms=[s.strip() for s in a.symbols.split(",")]
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    try:
        series={s: await fetch_closes(pool, s, a.days) for s in syms}
        as_of=datetime.utcnow().date()
        for A,B in itertools.combinations(syms,2):
            sA,sB=series[A].align(series[B], join="inner")
            if len(sA)<50: continue
            rho=float(pd.Series(sA).corr(pd.Series(sB)))
            hl=half_life(pd.Series(sA - sB))
            async with pool.acquire() as con:
                await con.execute("INSERT INTO pairs_candidates(as_of,sym_a,sym_b,rho,half_life) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                                  as_of, A, B, rho, hl)
            print(f"[pairs] {A}-{B} rho={rho:.3f} hl={hl}")
    finally:
        await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
