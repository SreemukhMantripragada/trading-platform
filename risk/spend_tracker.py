# risk/spend_tracker.py
from __future__ import annotations
import os, asyncpg, yaml, json, time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Tuple
from prometheus_client import Gauge

from risk.budget_source import compute_budgets   # uses broker cash + runtime YAML

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

RESERVED = Gauge("budget_reserved_inr", "Active reserved notional (INR)", ["bucket"])
HEADROOM = Gauge("budget_headroom_inr", "Budget headroom (INR)", ["bucket"])

class SpendTracker:
    """
    Usage:
      st = SpendTracker(pool)
      ok, rid, headroom = await st.try_reserve("MED", 20000.0, lease_sec=180, meta={"coid":coid})
      if ok: ... send order ...
      else:  ... drop / retry ...
    """
    def __init__(self, pool: asyncpg.pool.Pool):
        self.pool = pool

    async def _active_reserved(self, bucket:str) -> float:
        sql = """SELECT COALESCE(SUM(notional_inr),0.0) AS s
                 FROM budget_reservations
                 WHERE as_of = CURRENT_DATE AND bucket=$1 AND expires_at > now()"""
        async with self.pool.acquire() as con:
            r = await con.fetchval(sql, bucket)
        return float(r or 0.0)

    async def try_reserve(self, bucket:str, notional_inr:float, lease_sec:int=180, meta:dict|None=None) -> Tuple[bool, Optional[int], float]:
        budgets = compute_budgets()  # {'_meta':..., 'LOW':{'budget', 'max_per_trade'}, ...}
        bspec   = budgets.get(bucket, {"budget":0.0})
        capacity= float(bspec.get("budget", 0.0))
        active  = await self._active_reserved(bucket)
        headroom= max(0.0, capacity - active)

        # export gauges
        for b in ("LOW","MED","HIGH"):
            cap = float(budgets.get(b,{}).get("budget",0.0))
            act = await self._active_reserved(b) if b==bucket else act  # cheap approx; accurate for current bucket
            RESERVED.labels(b).set(act if b==bucket else float('nan'))
            HEADROOM.labels(b).set(max(0.0, cap - (act if b==bucket else 0.0)))

        if notional_inr > headroom + 1e-6:
            return False, None, headroom

        expires = datetime.now(timezone.utc) + timedelta(seconds=lease_sec)
        sql = """INSERT INTO budget_reservations(bucket, notional_inr, meta, expires_at)
                 VALUES($1,$2,$3,$4) RETURNING id"""
        async with self.pool.acquire() as con:
            rid = await con.fetchval(sql, bucket, float(notional_inr), json.dumps(meta or {}), expires)
        return True, int(rid), headroom - notional_inr

    async def cancel(self, rid:int) -> None:
        async with self.pool.acquire() as con:
            await con.execute("DELETE FROM budget_reservations WHERE id=$1", rid)
