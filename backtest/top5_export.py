"""
Export Top-5 configs from latest backtest run to configs/next_day.yaml
"""
from __future__ import annotations
import os, asyncpg, yaml, ujson as json

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

OUT="configs/next_day.yaml"

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    async with pool.acquire() as con:
        run=await con.fetchrow("SELECT run_id FROM backtest_runs ORDER BY started_at DESC LIMIT 1")
        if not run: 
            print("no runs"); return
        rid=int(run["run_id"])
        rows=await con.fetch(
            "SELECT symbol,strategy,tf_min,params,net_pnl FROM backtest_results WHERE run_id=$1 ORDER BY net_pnl DESC LIMIT 5", rid)
    out={"use": []}
    for r in rows:
        out["use"].append({
          "symbol": r["symbol"],
          "strategy": r["strategy"],
          "tf": int(r["tf_min"]),
          "params": json.loads(r["params"])
        })
    with open(OUT,"w") as f: yaml.safe_dump(out, f, sort_keys=False)
    print(f"wrote {OUT}")
    await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
