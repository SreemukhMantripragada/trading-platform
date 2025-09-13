"""
libs/cash.py
- Reads latest broker cash snapshot from Postgres (table: broker_cash).
- Computes deployable cash with live usage ratio (defaults to 0.80).
- Falls back to config starting cash if live is missing/stale.

Use:
  cash = CashRefresher(pool, usage_ratio=0.80, max_stale_sec=120)
  await cash.refresh()                        # call once or periodically
  dpl = cash.deployable(riskbud)              # returns float (INR)
"""
from __future__ import annotations
import time, asyncpg
from typing import Optional

class CashRefresher:
    def __init__(self, pool: asyncpg.Pool, usage_ratio: float = 0.80, max_stale_sec: int = 120):
        self.pool = pool
        self.usage_ratio = float(usage_ratio)
        self.max_stale_sec = int(max_stale_sec)
        self.live_avail: Optional[float] = None
        self.live_ts: Optional[float] = None

    async def refresh(self) -> None:
        row = None
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT equity_avail, extract(epoch from asof_ts) AS ts FROM broker_cash ORDER BY asof_ts DESC LIMIT 1"
            )
        if row and row["equity_avail"] is not None:
            self.live_avail = float(row["equity_avail"])
            self.live_ts = float(row["ts"])
        # else keep previous values (or None)

    def _live_ok(self) -> bool:
        if self.live_avail is None or self.live_ts is None:
            return False
        return (time.time() - self.live_ts) <= self.max_stale_sec

    def deployable(self, riskbud: dict) -> float:
        """
        Apply rule:
          - If live cash fresh: deploy = live_avail * broker_leverage_x * usage_ratio
          - Else: deploy = starting_cash_inr * broker_leverage_x * (1 - reserve_ratio)
        """
        cap = riskbud.get("capital", {})
        start_cash = float(cap.get("starting_cash_inr", 0.0))
        lev = float(cap.get("broker_leverage_x", 1.0))
        reserve_ratio = float(cap.get("reserve_ratio", 0.20))  # default to 20% reserve if not set

        if self._live_ok():
            return max(0.0, float(self.live_avail) * lev * self.usage_ratio)
        return max(0.0, start_cash * lev * (1.0 - reserve_ratio))
