# scripts/peek_results.py
import asyncio
from pprint import pprint

from backtest.persistence import get_latest_run_id, fetch_results
import asyncpg
import os

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

async def top_trades(run_id: int, limit: int = 10):
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB,
                                     user=PG_USER, password=PG_PASS, min_size=1, max_size=2)
    try:
        async with pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT symbol, strat AS strategy, variant, tf_min AS timeframe,
                       ts_entry, px_entry, qty, ts_exit, px_exit, pnl, reason
                FROM backtest_trades
                WHERE run_id = $1
                ORDER BY pnl DESC
                LIMIT $2
                """,
                run_id, limit
            )
            return [dict(r) for r in rows]
    finally:
        await pool.close()

async def main():
    rid = await get_latest_run_id()
    if not rid:
        print("No runs found.")
        return

    print(f"Latest run_id = {rid}")

    rows = await fetch_results([rid])
    print(f"Scored configs returned: {len(rows)}")

    print("\n=== Top 10 configs by score ===")
    for i, r in enumerate(rows[:10], 1):
        print(f"{i:2d}. {r['strategy']} | tf={r['timeframe']} | variant={r.get('variant','')} | sym={r['symbol']}")
        print(f"    score={r['score']:.3f}  R={r['R']:.2f}  S={r['sharpe']:.2f}  W={r['win_rate']:.1f}%  "
              f"DD={r['max_dd']:.2f}  U={r['ulcer']:.2f}  T={r['turnover']:.2f}  H={r['avg_hold_min']:.1f}m  "
              f"Stab={r['stability']:.3f}")
        # uncomment to see params
        # print(f"    params={r['params']}")

    print("\n=== Top 10 trades by |PnL| ===")
    trows = await top_trades(rid, 10)
    for i, t in enumerate(trows, 1):
        print(f"{i:2d}. {t['strategy']} | {t['symbol']} | tf={t['timeframe']} | pnl={t['pnl']:.2f} | reason={t['reason']}")
        print(f"    {t['ts_entry']} -> {t['ts_exit']}  qty={t['qty']}  {t['px_entry']} -> {t['px_exit']}")

if __name__ == "__main__":
    asyncio.run(main())
