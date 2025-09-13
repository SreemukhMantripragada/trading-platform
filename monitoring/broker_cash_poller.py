"""
Poll Zerodha funds -> store snapshots in Postgres.

Env:
  KITE_API_KEY=xxxx
  ZERODHA_TOKEN_FILE=ingestion/auth/token.json
  POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=trading POSTGRES_USER=trader POSTGRES_PASSWORD=trader
  POLL_SEC=20
"""
from __future__ import annotations
import os, asyncio, json as pyjson
from datetime import datetime, timezone
from dotenv import load_dotenv

import asyncpg
from kiteconnect import KiteConnect, exceptions as kz_ex

load_dotenv(".env"); load_dotenv("infra/.env")

KITE_API_KEY=os.getenv("KITE_API_KEY")
TOKEN_FILE=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

POLL_SEC=int(os.getenv("POLL_SEC","20"))

UPSERT_SQL = """
INSERT INTO broker_cash(asof_ts, source, account_id, equity_used, equity_avail, mtm, raw)
VALUES (now(), 'zerodha', $1, $2, $3, $4, $5::jsonb)
"""

def _load_access_token() -> str:
    with open(TOKEN_FILE) as f:
        t = pyjson.load(f)
    if t.get("api_key") != KITE_API_KEY:
        raise SystemExit("Token file API key mismatch. Re-auth.")
    return t["access_token"]

async def main():
    access = _load_access_token()
    kite = KiteConnect(api_key=KITE_API_KEY)
    kite.set_access_token(access)

    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    print(f"[cash_poller] polling every {POLL_SEC}s → broker_cash")

    while True:
        try:
            prof = kite.profile()        # user_id, etc.
            funds = kite.margins("equity")
            # Zerodha returns fields like 'net', 'available', etc.
            acct = prof.get("user_id", None)
            avail = None
            used  = None
            mtm   = None
            try:
                avail = float(funds.get("available", {}).get("cash", 0.0))
            except Exception:
                avail = float(funds.get("net", 0.0))
            try:
                used = float(funds.get("utilised", {}).get("debits", 0.0))
            except Exception:
                used = None
            try:
                mtm = float(funds.get("m2m_unrealised", 0.0))
            except Exception:
                mtm = None

            async with pool.acquire() as con:
                await con.execute(
                    UPSERT_SQL, acct, used, avail, mtm, pyjson.dumps({"profile": prof, "funds": funds})
                )
            print(f"[cash_poller] acct={acct} avail≈₹{avail:.2f}")

        except kz_ex.TokenException:
            print("[cash_poller] Token invalid/expired. Re-auth needed.")
        except Exception as e:
            print(f"[cash_poller] error: {e}")

        await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
