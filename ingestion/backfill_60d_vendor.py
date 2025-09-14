# ingestion/backfill_60d_vendor.py
# One-shot (per symbol) backfill of last 60 days of 1m candles from Zerodha.
# - Validates token, loads enabled symbols from configs/tokens.csv (subscribe=1)
# - Requests 60d window (09:15–15:30 IST each day) in a single API call per symbol
# - Fills internal missing minutes by linear interpolation ("SynthAvg")
# - UPSERTs into bars_1m_golden (Zerodha or SynthAvg), idempotent
# - Built-in sleep + exponential backoff on rate limits (no CLI args)

from __future__ import annotations
import os, csv, time, random
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Tuple
import asyncpg
from dotenv import load_dotenv

# ---------- Hard defaults (no CLI) ----------
DAYS_BACK            = 60       # fetch last 60 days
RESUME_FROM_DB       = True     # move start forward to last ts in DB per symbol
SLEEP_BETWEEN_SYMBOL = 1.0      # seconds between symbols
BASE_SLEEP_MS        = 800      # base for backoff on 429 (ms)
MAX_RETRIES          = 8        # retries per symbol on rate limit
GAP_FILL             = True     # interpolate internal gaps

# ---------- ENV ----------
load_dotenv(".env"); load_dotenv("infra/.env")

KITE_API_KEY = os.getenv("KITE_API_KEY")
TOKEN_FILE   = os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")
TOKENS_CSV   = os.getenv("ZERODHA_TOKENS_CSV","configs/tokens.csv")

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

# ---------- DDL & UPSERT ----------
DDL = """
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='public' AND table_name='bars_1m_golden') THEN
    CREATE TABLE public.bars_1m_golden(
      symbol    text         NOT NULL,
      ts        timestamptz  NOT NULL,
      o         double precision,
      h         double precision,
      l         double precision,
      c         double precision,
      vol       bigint,
      source    text DEFAULT 'Zerodha',
      loaded_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY(symbol, ts)
    );
    CREATE INDEX IF NOT EXISTS bars_1m_golden_ts_idx ON public.bars_1m_golden(ts);
  END IF;
END$$;
"""

UPSERT_SQL = """
INSERT INTO bars_1m_golden(symbol, ts, o,h,l,c,vol, source)
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (symbol, ts) DO UPDATE
SET o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c,
    vol=EXCLUDED.vol, source=EXCLUDED.source, loaded_at=now();
"""

# ---------- Zerodha helpers ----------
def _load_kite():
    from kiteconnect import KiteConnect
    import json as _j
    if not KITE_API_KEY:
        raise SystemExit("KITE_API_KEY missing in env")
    try:
        t=_j.load(open(TOKEN_FILE))
    except Exception as e:
        raise SystemExit(f"Token load failed: {e}")
    access=os.getenv("KITE_ACCESS_TOKEN") or t.get("access_token")
    if not access:
        raise SystemExit("Missing access_token (env KITE_ACCESS_TOKEN or token.json)")
    k=KiteConnect(api_key=KITE_API_KEY)
    k.set_access_token(access)
    k.profile()  # raises if invalid
    return k

def _load_tokens_from_csv() -> List[Tuple[int,str]]:
    out=[]
    with open(TOKENS_CSV, newline="") as f:
        r=csv.DictReader(f)
        for row in r:
            if (row.get("subscribe","1") or "1").strip().lower() not in ("1","true","yes","y"):
                continue
            out.append((int(row["instrument_token"]), row["tradingsymbol"].strip()))
    if not out:
        raise SystemExit(f"No enabled symbols in {TOKENS_CSV}")
    return out

# ---------- Time windows ----------
def ist_window_utc(day: date):
    IST = timezone(timedelta(hours=5, minutes=30))
    s = datetime.combine(day, datetime.min.time(), IST).replace(hour=9, minute=15)
    e = datetime.combine(day, datetime.min.time(), IST).replace(hour=15, minute=30)
    return s.astimezone(timezone.utc), e.astimezone(timezone.utc)

def span_ist_utc(start_day: date, end_day: date):
    s_utc, _ = ist_window_utc(start_day)
    _, e_utc = ist_window_utc(end_day)
    return s_utc, e_utc

# ---------- Backoff / rate-limit ----------
def _rate_limited(e: Exception) -> bool:
    msg = str(e).lower()
    return ("too many request" in msg) or ("429" in msg) or ("rate" in msg)

def _backoff_sleep(base_ms: int, attempt: int):
    delay = (base_ms/1000.0) * (2 ** attempt) + random.uniform(0, 0.25)
    time.sleep(delay)

# ---------- Gap filling ----------
def _to_minute(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc)
    return ts.replace(second=0, microsecond=0)

def _lin(a: float, b: float, t: float) -> float:
    return a + (b - a) * t

def _fill_missing_minutes(candles: List[dict], start_utc: datetime, end_utc: datetime
                         ) -> List[Tuple[datetime, float,float,float,float,int,str]]:
    by_ts: Dict[datetime, dict] = {}
    for c in candles or []:
        ts = _to_minute(c["date"])
        by_ts[ts] = c  # last one wins if any dup

    known = sorted(ts for ts in by_ts if start_utc <= ts <= end_utc)
    out: List[Tuple[datetime, float,float,float,float,int,str]] = []
    if not known:
        return out

    def add_real(ts: datetime, c: dict):
        out.append((
            ts, float(c["open"]), float(c["high"]), float(c["low"]),
            float(c["close"]), int(c.get("volume") or 0), "Zerodha"
        ))

    add_real(known[0], by_ts[known[0]])
    for i in range(len(known)-1):
        a, b = known[i], known[i+1]
        dtm = int((b - a).total_seconds() // 60)
        if dtm <= 1:
            add_real(b, by_ts[b])
            continue
        ca, cb = by_ts[a], by_ts[b]
        steps = dtm
        for k in range(1, dtm):
            t = k/steps
            o = _lin(float(ca["open"]),  float(cb["open"]),  t)
            h = _lin(float(ca["high"]),  float(cb["high"]),  t)
            l = _lin(float(ca["low"]),   float(cb["low"]),   t)
            c = _lin(float(ca["close"]), float(cb["close"]), t)
            v = int(round(_lin(float(ca.get("volume") or 0), float(cb.get("volume") or 0), t)))
            out.append((a + timedelta(minutes=k), o,h,l,c,v, "SynthAvg"))
        add_real(b, by_ts[b])

    return out

# ---------- Main ----------
async def main():
    today = date.today()
    start_day = today - timedelta(days=DAYS_BACK)
    end_day   = today - timedelta(days=1)

    # Connectors
    kite = _load_kite()
    tokens = _load_tokens_from_csv()
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB,
                                     user=PG_USER, password=PG_PASS)
    async with pool.acquire() as con:
        await con.execute(DDL)

    total_real = total_synth = 0

    try:
        for tok, sym in tokens:
            sym_start = start_day
            if RESUME_FROM_DB:
                async with pool.acquire() as con:
                    last_ts = await con.fetchval(
                        "SELECT max(ts) FROM bars_1m_golden WHERE symbol=$1", sym
                    )
                if last_ts and last_ts.date() > sym_start:
                    sym_start = last_ts.date()
                    # If sym_start moved past end_day, skip symbol
                    if sym_start > end_day:
                        print(f"[skip up-to-date] {sym}")
                        continue
                    print(f"[resume] {sym}: start→{sym_start}")

            from_utc, to_utc = span_ist_utc(sym_start, end_day)

            attempt = 0
            while True:
                try:
                    raw = kite.historical_data(tok, from_utc, to_utc, "minute",
                                               continuous=False, oi=False) or []
                    if GAP_FILL:
                        rows = _fill_missing_minutes(raw, from_utc, to_utc)
                    else:
                        rows = []
                        for c in raw:
                            ts = c["date"]
                            rows.append((ts, float(c["open"]), float(c["high"]),
                                         float(c["low"]), float(c["close"]),
                                         int(c.get("volume") or 0), "Zerodha"))

                    to_write = [(sym, ts, o,h,l,c,vol, src) for (ts,o,h,l,c,vol,src) in rows]
                    if to_write:
                        async with pool.acquire() as con:
                            await con.executemany(UPSERT_SQL, to_write)
                        n_real  = sum(1 for r in to_write if r[-1] == "Zerodha")
                        n_synth = len(to_write) - n_real
                        total_real  += n_real
                        total_synth += n_synth
                        print(f"[60d] {sym} {sym_start}→{end_day} upserts={len(to_write)} real={n_real} synth={n_synth}")
                    else:
                        print(f"[60d] {sym} {sym_start}→{end_day} empty")

                    time.sleep(SLEEP_BETWEEN_SYMBOL)
                    break
                except Exception as e:
                    if _rate_limited(e) and attempt < MAX_RETRIES:
                        print(f"[rate-limit] {sym} attempt={attempt+1}/{MAX_RETRIES}: {e}")
                        _backoff_sleep(BASE_SLEEP_MS, attempt)
                        attempt += 1
                        continue
                    print(f"[error] {sym}: {e}")
                    break

    finally:
        await pool.close()
        print(f"Done. total_real={total_real} total_synth={total_synth}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
