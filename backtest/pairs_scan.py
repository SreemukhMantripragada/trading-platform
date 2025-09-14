# backtest/pairs_scan.py
# Discover cointegrated pairs from bars_1m; store results; optionally select top-K for tomorrow's live.
from __future__ import annotations
import os, asyncio
from datetime import datetime, timedelta, timezone, date
import asyncpg
import numpy as np
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
from dotenv import load_dotenv

load_dotenv(".env"); load_dotenv("infra/.env")

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

UNIVERSE = (os.getenv("PAIRS_UNIVERSE","RELIANCE,TCS,HDFCBANK,INFY,ICICIBANK")).split(",")
LOOKBACK_DAYS = int(os.getenv("PAIRS_LOOKBACK_DAYS","45"))
PROMOTE_TOP_K = int(os.getenv("PAIRS_PROMOTE_TOP_K","8"))  # how many pairs to push to pairs_live
ENTRY_Z = float(os.getenv("PAIRS_ENTRY_Z","2.0"))
EXIT_Z  = float(os.getenv("PAIRS_EXIT_Z","0.5"))
STOP_Z  = float(os.getenv("PAIRS_STOP_Z","4.0"))
WINDOW_M= int(os.getenv("PAIRS_WINDOW_M","120"))  # z-score rolling window in minutes

SQL_LOAD = """
SELECT symbol, ts, c
FROM bars_1m
WHERE symbol = ANY($1::text[]) AND ts >= $2 AND ts < $3
ORDER BY symbol, ts
"""

SQL_INS = """
INSERT INTO pairs_scan(scan_date, sym1, sym2, pvalue, beta, half_life_m, zmean, zstd, bars_used)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (scan_date, sym1, sym2) DO UPDATE
SET pvalue=EXCLUDED.pvalue, beta=EXCLUDED.beta, half_life_m=EXCLUDED.half_life_m,
    zmean=EXCLUDED.zmean, zstd=EXCLUDED.zstd, bars_used=EXCLUDED.bars_used
"""

SQL_PROMOTE = """
INSERT INTO pairs_live(trade_date, sym1, sym2, beta, entry_z, exit_z, stop_z, window_m)
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (trade_date, sym1, sym2) DO UPDATE
SET beta=EXCLUDED.beta, entry_z=EXCLUDED.entry_z, exit_z=EXCLUDED.exit_z,
    stop_z=EXCLUDED.stop_z, window_m=EXCLUDED.window_m
"""

def half_life_minutes(spread: np.ndarray, freq_per_min=1.0) -> float:
    # AR(1) regression: Δs_t = a + b*s_{t-1} + e_t -> half-life = -ln(2)/ln(1+b)
    s = spread
    if len(s) < 30: return float("nan")
    y = np.diff(s)
    x = s[:-1]
    x = sm.add_constant(x)
    res = sm.OLS(y, x).fit()
    b = res.params[1]
    try:
        hl = -np.log(2) / np.log(1 + b)
        if not np.isfinite(hl): return float("nan")
        return float(hl) / freq_per_min
    except Exception:
        return float("nan")

async def main():
    end = datetime.now(timezone.utc).replace(hour=10, minute=0, second=0, microsecond=0)  # ~post-market in UTC
    start = end - timedelta(days=LOOKBACK_DAYS)
    scan_date = date.today()

    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        # Load close series
        async with pool.acquire() as con:
            rows = await con.fetch(SQL_LOAD, UNIVERSE, start, end)
        # pack by symbol
        series = {}
        for r in rows:
            series.setdefault(r["symbol"], []).append(float(r["c"]))
        # align to equal length (truncate to min length)
        minlen = min(len(v) for v in series.values() if v)
        for k in list(series.keys()):
            series[k] = np.array(series[k][-minlen:], dtype=float)

        results = []
        syms = sorted(series.keys())
        for i in range(len(syms)):
            for j in range(i+1, len(syms)):
                s1, s2 = syms[i], syms[j]
                x = series[s1]; y = series[s2]
                if len(x) < 100 or len(y) < 100: 
                    continue
                # hedge: y ~ beta*x
                beta = (np.dot(x, y) / np.dot(x, x)) if np.dot(x,x)!=0 else 0.0
                spread = y - beta * x
                # cointegration p-value
                _, pval, _ = coint(y, x, trend='c')
                # zscore params
                z = (spread - spread.mean()) / (spread.std() + 1e-9)
                zmean = float(np.mean(z)); zstd = float(np.std(z))
                # half-life in minutes (freq=1/min since bars_1m)
                hl = half_life_minutes(spread, freq_per_min=1.0)
                results.append((pval, s1, s2, beta, hl, zmean, zstd, len(spread)))

        # sort by p-value then shorter half-life
        results.sort(key=lambda r: (r[0], r[4] if np.isfinite(r[4]) else 1e9))
        async with pool.acquire() as con:
            async with con.transaction():
                for (pval, s1, s2, beta, hl, zmean, zstd, n) in results:
                    await con.execute(SQL_INS, scan_date, s1, s2, float(pval), float(beta),
                                      (None if not np.isfinite(hl) else float(hl)),
                                      float(zmean), float(zstd), int(n))
                # promote top-K to pairs_live for *next* session
                top = results[:PROMOTE_TOP_K]
                trade_date = scan_date  # same-day if run pre-open; adjust as you prefer
                for (_, s1, s2, beta, _, _, _, _) in top:
                    await con.execute(SQL_PROMOTE, trade_date, s1, s2, float(beta),
                                      ENTRY_Z, EXIT_Z, STOP_Z, WINDOW_M)
        print(f"[pairs_scan] wrote {len(results)} rows; promoted {min(PROMOTE_TOP_K,len(results))} pairs for {scan_date}")
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
