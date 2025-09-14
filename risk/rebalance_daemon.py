"""
risk/rebalance_daemon.py
Recomputes LOW/MED/HIGH splits every minute using fills-derived equity.

Policy (simple):
- Start with base splits from configs/risk_budget.yaml
- If bucket DD% < -2%, reduce its split by 25% (redistribute pro-rata).
- If bucket DD% < -4%, reduce by 50%.
- Always keep total deploy <= 80% of live cash (runner enforces).

Writes configs/risk_budget.runtime.yaml atomically.

Run:
  python risk/rebalance_daemon.py
"""
from __future__ import annotations
import os, asyncio, asyncpg, yaml
from datetime import date

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
BASE=os.getenv("BASE_RISK","configs/risk_budget.yaml")
OUT=os.getenv("OUT_RISK","configs/risk_budget.runtime.yaml")

async def dd_by_bucket(con):
    rows = await con.fetch("""
      WITH px AS (
        SELECT DISTINCT ON (symbol) symbol, c FROM bars_1m ORDER BY symbol, ts DESC
      ),
      pos AS (
        SELECT symbol, side, qty, price, coalesce(risk_bucket,'MED') as bucket
        FROM fills WHERE ts::date=$1
      )
      SELECT p.bucket,
             0.0::float8 AS dd_pct -- placeholder; swap to your exporter table if available
      FROM (SELECT DISTINCT coalesce(risk_bucket,'MED') bucket FROM fills WHERE ts::date=$1) p
    """, date.today())
    # If you have risk_drawdown_exporter table/metrics in DB, replace the query above & compute dd_pct.
    d={r["bucket"]: float(r["dd_pct"]) for r in rows}
    for b in ("LOW","MED","HIGH"):
        d.setdefault(b, 0.0)
    return d

def reweight(base:dict, dd:dict):
    splits=base["buckets"].copy()
    scalers={"LOW":1.0,"MED":1.0,"HIGH":1.0}
    for b, v in dd.items():
        if v <= -4.0: scalers[b]=0.5
        elif v <= -2.0: scalers[b]=0.75
    total = sum(float(splits[b]["split"])*scalers[b] for b in splits)
    if total <= 0: return base
    for b in splits:
        splits[b]["split"] = round(float(splits[b]["split"])*scalers[b]/total, 4)
    base2=dict(base); base2["buckets"]=splits
    return base2

async def main():
    base=yaml.safe_load(open(BASE))
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        while True:
            async with pool.acquire() as con:
                dd=await dd_by_bucket(con)
            cfg=reweight(base, dd)
            tmp=OUT+".tmp"
            with open(tmp,"w") as f: yaml.safe_dump(cfg, f, sort_keys=False)
            os.replace(tmp, OUT)
            print("[rebalance] wrote", OUT, "splits=", {k:v["split"] for k,v in cfg["buckets"].items()})
            await asyncio.sleep(60)
    finally:
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
