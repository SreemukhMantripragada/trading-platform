# accounting/position_tracker.py
from __future__ import annotations
import asyncpg, os
from typing import Dict, Tuple

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

class PositionTracker:
    """
    Computes intraday net positions by symbol (BUY qty - SELL qty).
    Call refresh() periodically; read with get(symbol) -> (net_qty, avg_cost).
    """
    def __init__(self, pool: asyncpg.pool.Pool):
        self.pool = pool
        self.pos: Dict[str, Tuple[int, float]] = {}

    async def refresh(self):
        sql = """
        WITH f AS (
          SELECT symbol,
                 SUM(CASE WHEN side='BUY'  THEN qty ELSE 0 END) AS bq,
                 SUM(CASE WHEN side='BUY'  THEN qty*price ELSE 0 END) AS bv,
                 SUM(CASE WHEN side='SELL' THEN qty ELSE 0 END) AS sq,
                 SUM(CASE WHEN side='SELL' THEN qty*price ELSE 0 END) AS sv
          FROM fills WHERE ts::date = CURRENT_DATE
          GROUP BY symbol
        )
        SELECT symbol,
               (bq - sq) AS net_qty,
               CASE WHEN bq>0 THEN (bv/bq) ELSE 0 END AS avg_cost
        FROM f;
        """
        async with self.pool.acquire() as con:
            rows = await con.fetch(sql)
        self.pos = { r["symbol"]: (int(r["net_qty"] or 0), float(r["avg_cost"] or 0.0)) for r in rows }

    def get(self, symbol:str)->Tuple[int,float]:
        return self.pos.get(symbol, (0,0.0))
