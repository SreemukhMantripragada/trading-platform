"""
execution/accounting_pnl.py
- Computes live realized/unrealized P&L per symbol & bucket from orders/fills.
- Applies costs from configs/costs.yaml (brokerage, taxes, slippage).
- Persists to Postgres tables (see 09_accounting.sql).
- Exposes Prometheus metrics.

Assumptions:
  - orders(symbol, client_order_id, side, qty, price, risk_bucket, created_at, strategy)
  - fills(symbol, client_order_id, side, qty, price, ts, strategy, risk_bucket)
  - bars_1m(symbol, ts, c) for MTM snapshots

ENV:
  POSTGRES_*, METRICS_PORT=8024
  COSTS_YAML=configs/costs.yaml
"""
from __future__ import annotations
import os, asyncpg, yaml, math
from datetime import datetime, timezone
from prometheus_client import start_http_server, Gauge

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
PORT=int(os.getenv("METRICS_PORT","8024"))
COSTS_YAML=os.getenv("COSTS_YAML","configs/costs.yaml")

# ---- Prometheus exported metrics ----
PNL_REAL  = Gauge("acct_pnl_realized_inr",   "Realized PnL (INR)", ["symbol","bucket"])
PNL_UNREAL= Gauge("acct_pnl_unrealized_inr", "Unrealized PnL (INR)", ["symbol","bucket"])
EQUITY    = Gauge("acct_equity_inr",         "Account equity (sum realized+unrealized)", ["bucket"])

def load_costs():
    """
    Returns dict with slippage_bps, brokerage_per_order, sebi_turnover_bps, stt_bps,
    exchange_txn_bps, gst_bps_on_brokerage, stamp_bps_buy
    """
    with open(COSTS_YAML) as f:
        c=yaml.safe_load(f) or {}
    # Ensure floats
    out={}
    for k,v in (c or {}).items():
        try: out[k]=float(v)
        except: out[k]=0.0
    return out

async def fetch_last_px(con, sym:str)->float:
    r=await con.fetchrow("SELECT c FROM bars_1m WHERE symbol=$1 ORDER BY ts DESC LIMIT 1", sym)
    return float((r or {}).get("c") or 0.0)

def round2(x:float)->float: return float(f"{x:.2f}")

def fill_costs(costs:dict, side:str, qty:int, px:float)->float:
    """
    Simple Indian cash-equity cost model (approx):
      - Slippage: px * bps
      - Brokerage: flat per order
      - Turnover/Exchange bps on notional
      - GST on brokerage
      - Stamp duty on BUY only
    """
    notional=px*qty
    slippage = notional * (costs.get("slippage_bps",0.0)/10000.0)
    brokerage= costs.get("brokerage_per_order",0.0)
    sebi     = notional * (costs.get("sebi_turnover_bps",0.0)/10000.0)
    exch     = notional * (costs.get("exchange_txn_bps",0.0)/10000.0)
    gst      = brokerage * (costs.get("gst_bps_on_brokerage",0.0)/10000.0)
    stamp    = (notional * (costs.get("stamp_bps_buy",0.0)/10000.0)) if side=="BUY" else 0.0
    return slippage + brokerage + sebi + exch + gst + stamp

async def loop():
    costs = load_costs()
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        while True:
            try:
                async with pool.acquire() as con:
                    # Net positions & realized pnl per symbol/bucket today from fills
                    rows = await con.fetch("""
                      WITH f AS (
                        SELECT symbol, risk_bucket,
                               SUM(CASE WHEN side='BUY'  THEN qty ELSE 0 END) AS buy_qty,
                               SUM(CASE WHEN side='BUY'  THEN qty*price ELSE 0 END) AS buy_val,
                               SUM(CASE WHEN side='SELL' THEN qty ELSE 0 END) AS sell_qty,
                               SUM(CASE WHEN side='SELL' THEN qty*price ELSE 0 END) AS sell_val,
                               COUNT(*) AS n_fills
                        FROM fills WHERE ts::date = CURRENT_DATE
                        GROUP BY 1,2
                      ),
                      realized AS (
                        -- FIFO/average approx realized PnL: sell_val - average_cost * sell_qty
                        SELECT symbol, risk_bucket,
                               sell_val - (CASE WHEN (buy_qty>0) THEN (buy_val/buy_qty)*sell_qty ELSE 0 END) AS realized_pnl,
                               (buy_qty - sell_qty) AS net_qty,
                               (CASE WHEN buy_qty>0 THEN buy_val/buy_qty ELSE 0 END) AS avg_cost,
                               n_fills
                        FROM f
                      )
                      SELECT * FROM realized;
                    """)
                    all_equity = {"LOW":0.0,"MED":0.0,"HIGH":0.0}
                    for r in rows:
                        sym=r["symbol"]; b=(r["risk_bucket"] or "MED").upper()
                        net_qty=int(r["net_qty"] or 0)
                        realized=float(r["realized_pnl"] or 0.0)

                        # apply per-fill average cost adjustments crudely via fill count * brokerage; slippage handled inside fills
                        # For better accuracy, costs should be added per actual fill insert time.
                        extra_cost = r["n_fills"] * costs.get("brokerage_per_order",0.0)
                        realized -= extra_cost

                        mtm=0.0
                        if net_qty != 0:
                            last_px = await fetch_last_px(con, sym) or r["avg_cost"]
                            mtm = (last_px - float(r["avg_cost"])) * net_qty

                        # Persist snapshot
                        await con.execute("""
                          INSERT INTO pnl_intraday(as_of, symbol, bucket, realized_inr, unrealized_inr)
                          VALUES (CURRENT_DATE, $1, $2, $3, $4)
                          ON CONFLICT (as_of,symbol,bucket) DO UPDATE
                          SET realized_inr=EXCLUDED.realized_inr, unrealized_inr=EXCLUDED.unrealized_inr, updated_at=now()
                        """, sym, b, round2(realized), round2(mtm))

                        # Export metrics
                        PNL_REAL.labels(sym,b).set(round2(realized))
                        PNL_UNREAL.labels(sym,b).set(round2(mtm))
                        all_equity[b]+=realized+mtm

                    for bucket, eq in all_equity.items():
                        EQUITY.labels(bucket).set(round2(eq))
            except Exception as e:
                print("[acct] error:", e)
            # update every 5s
            import asyncio; await asyncio.sleep(5)
    finally:
        await pool.close()

def main():
    start_http_server(PORT)
    print(f"[acct] exporter on :{PORT}/metrics using {COSTS_YAML}")
    import asyncio; asyncio.run(loop())

if __name__=="__main__":
    main()
