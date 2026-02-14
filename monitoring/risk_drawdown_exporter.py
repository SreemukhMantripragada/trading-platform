"""
monitoring/risk_drawdown_exporter.py
Exports intraday equity & drawdown per risk-bucket.

Assumptions:
- Postgres tables: fills(symbol, ts, side, qty, price, strategy, risk_bucket)
- Latest prices from bars_1m (one price per symbol, most recent row)
- Buckets are strings like LOW/MED/HIGH

Metrics:
- risk_equity_inr{bucket}
- risk_drawdown_pct{bucket}
- risk_dd_velocity_pct_per_min{bucket}

Run:
  POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=trading POSTGRES_USER=trader POSTGRES_PASSWORD=trader \
  METRICS_PORT=8019 python monitoring/risk_drawdown_exporter.py
"""
from __future__ import annotations
import os, asyncio, asyncpg
from collections import deque
from datetime import date
from prometheus_client import start_http_server, Gauge

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
METRICS_PORT=int(os.getenv("METRICS_PORT","8019"))

EQUITY = Gauge("risk_equity_inr", "Intraday equity (realized+MTM) per bucket", ["bucket"])
DD     = Gauge("risk_drawdown_pct", "Max drawdown (%) from today's peak per bucket", ["bucket"])
DDV    = Gauge("risk_dd_velocity_pct_per_min", "Drawdown velocity (%/min) recent", ["bucket"])

async def fetch_fills(con):
    # today’s fills, ordered
    return await con.fetch("""
      SELECT symbol, ts, side, qty, price, coalesce(risk_bucket,'MED') AS bucket
      FROM fills
      WHERE ts::date = $1::date
      ORDER BY ts
    """, date.today())

async def fetch_last_px(con):
    rows = await con.fetch("""
      SELECT DISTINCT ON (symbol) symbol, c
      FROM bars_1m
      ORDER BY symbol, ts DESC
    """)
    return {r["symbol"]: float(r["c"]) for r in rows}

def fifo_pnl_and_pos(fills, last_px_map):
    """
    Returns:
      bucket -> dict(equity, drawdown_pct, dd_velocity_pct_per_min)
    """
    # per-bucket state
    state = {}
    # keep last two equity points to estimate velocity
    last_equity_points = {}

    for f in fills:
        b = f["bucket"]; sym = f["symbol"]; side=f["side"]; qty=int(f["qty"]); px=float(f["price"])
        st = state.setdefault(b, {"lots":{}, "realized":0.0})
        lots = st["lots"].setdefault(sym, deque())  # each lot: (qty_remaining, avg_cost)
        if side == "BUY":
            lots.append([qty, px])
        else:  # SELL
            to_sell = qty
            while to_sell > 0 and lots:
                q0, cost = lots[0]
                take = min(q0, to_sell)
                st["realized"] += (px - cost) * take
                q0 -= take; to_sell -= take
                if q0 == 0: lots.popleft()
                else: lots[0][0] = q0
            # if selling more than position (shouldn't), ignore the extra

    out={}
    for b, st in state.items():
        mtm=0.0
        for sym, lots in st["lots"].items():
            px = last_px_map.get(sym)
            if px is None: continue
            for q, cost in lots:
                mtm += (px - cost) * q
        eq = st["realized"] + mtm
        # peak/velocity tracking
        lp = last_equity_points.setdefault(b, deque(maxlen=5))
        lp.append(eq)
        peak = max(lp) if lp else eq
        dd_pct = 0.0 if peak == 0 else min(0.0, (eq - peak) / abs(peak)) * 100.0
        vel = 0.0
        if len(lp) >= 2:
            vel = ((lp[-1] - lp[0]) / max(abs(peak), 1e-6)) * 100.0 / max(len(lp)-1, 1)  # % per sample (~5s)
            # rough normalize to per-minute assuming scrape ~5s:
            vel *= (60.0/5.0)
        out[b] = {"equity": eq, "dd_pct": dd_pct, "vel": vel}
    return out

async def scrape(pool):
    async with pool.acquire() as con:
        fills = await fetch_fills(con)
        last_px_map = await fetch_last_px(con)
    by_bucket = fifo_pnl_and_pos(fills, last_px_map)
    # publish
    for b, v in by_bucket.items():
        EQUITY.labels(b).set(v["equity"])
        DD.labels(b).set(v["dd_pct"])
        DDV.labels(b).set(v["vel"])
    # ensure labels exist for LOW/MED/HIGH even if zero
    for b in ("LOW","MED","HIGH"):
        if b not in by_bucket:
            EQUITY.labels(b).set(0.0); DD.labels(b).set(0.0); DDV.labels(b).set(0.0)

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    start_http_server(METRICS_PORT)
    print(f"[dd-exp] :{METRICS_PORT}/metrics")
    try:
        while True:
            try: await scrape(pool)
            except Exception as e: print("[dd-exp] scrape error:", e)
            await asyncio.sleep(5)
    finally:
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
