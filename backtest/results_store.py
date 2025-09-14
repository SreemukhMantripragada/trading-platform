"""
backtest/results_store.py
Persists backtest results; fetches Top-N; creates schema on first use.

Tables:
  bt_runs(run_id, created_at, tf, strategy, params_json, universe, notes)
  bt_metrics(run_id, symbol, pnl, pnl_pct, sharpe, max_dd_pct, trades, win_rate, avg_hold_min)

Usage:
  from backtest.results_store import ResultsStore
  rs = ResultsStore()           # reads POSTGRES_* env
  run_id = await rs.save_run(tf="1m", strategy="RSI_REV", params={"period":14}, universe="NIFTY100", notes="grid#42", rows=[...])
  top = await rs.top_n(tf="1m", metric="sharpe", n=5, min_trades=20)
"""
from __future__ import annotations
import os, asyncpg, json
from typing import List, Dict, Any

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

DDL = """
CREATE TABLE IF NOT EXISTS bt_runs(
  run_id      bigserial PRIMARY KEY,
  created_at  timestamptz NOT NULL DEFAULT now(),
  tf          text NOT NULL,
  strategy    text NOT NULL,
  params_json jsonb NOT NULL,
  universe    text NOT NULL,
  notes       text
);
CREATE TABLE IF NOT EXISTS bt_metrics(
  run_id   bigint REFERENCES bt_runs(run_id) ON DELETE CASCADE,
  symbol   text NOT NULL,
  pnl      double precision NOT NULL,
  pnl_pct  double precision NOT NULL,
  sharpe   double precision,
  max_dd_pct double precision,
  trades   int NOT NULL,
  win_rate double precision,
  avg_hold_min double precision
);
CREATE INDEX IF NOT EXISTS bt_runs_tf_idx ON bt_runs(tf);
CREATE INDEX IF NOT EXISTS bt_metrics_run_idx ON bt_metrics(run_id);
"""

class ResultsStore:
    def __init__(self):
        self._pool=None

    async def _pool_get(self):
        if self._pool: return self._pool
        self._pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
        async with self._pool.acquire() as con:
            await con.execute(DDL)
        return self._pool

    async def save_run(self, tf:str, strategy:str, params:Dict[str,Any], universe:str, notes:str, rows:List[Dict[str,Any]]) -> int:
        """
        rows: [{symbol, pnl, pnl_pct, sharpe, max_dd_pct, trades, win_rate, avg_hold_min}]
        returns run_id
        """
        pool=await self._pool_get()
        async with pool.acquire() as con:
            async with con.transaction():
                r = await con.fetchrow(
                    "INSERT INTO bt_runs(tf,strategy,params_json,universe,notes) VALUES($1,$2,$3,$4,$5) RETURNING run_id",
                    tf, strategy, json.dumps(params), universe, notes
                )
                run_id=int(r["run_id"])
                await con.executemany(
                    "INSERT INTO bt_metrics(run_id,symbol,pnl,pnl_pct,sharpe,max_dd_pct,trades,win_rate,avg_hold_min) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    [(run_id, row["symbol"], float(row["pnl"]), float(row["pnl_pct"]), row.get("sharpe"), row.get("max_dd_pct"),
                      int(row.get("trades",0)), row.get("win_rate"), row.get("avg_hold_min")) for row in rows]
                )
        return run_id

    async def top_n(self, tf:str, metric:str="sharpe", n:int=5, min_trades:int=10):
        """Returns list of {run_id, strategy, params_json, universe, notes, agg_metric} ordered desc."""
        assert metric in ("sharpe","pnl","pnl_pct","win_rate"), "unsupported metric"
        pool=await self._pool_get()
        q=f"""
          SELECT r.run_id, r.strategy, r.params_json, r.universe, r.notes,
                 AVG(m.{metric}) AS agg_metric, SUM(m.trades) AS total_trades
          FROM bt_runs r
          JOIN bt_metrics m USING(run_id)
          WHERE r.tf=$1
          GROUP BY r.run_id
          HAVING SUM(m.trades) >= $2
          ORDER BY agg_metric DESC
          LIMIT $3
        """
        async with pool.acquire() as con:
            rows=await con.fetch(q, tf, min_trades, n)
        return [dict(row) for row in rows]
