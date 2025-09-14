# ingestion/backfill_60d_vendor.py
# One-shot 60d 1m backfill:
#  - Fetch full 60-day window per symbol from Zerodha (single call)
#  - Day-bounded interpolation of ALL missing intraday minutes (09:15–15:30 IST), no cross-day fill
#  - Load ALL existing DB rows for the same window; compare every minute (tolerances)
#  - UPSERT only missing/changed rows; log inserts/updates/skips; count real vs synth writes
#  - Gentle rate-limit backoff (no CLI flags needed)

from __future__ import annotations
import os, csv, time, random
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Tuple
import asyncpg
from dotenv import load_dotenv

# -------- Constants (no CLI flags) --------
DAYS_BACK            = 60
SLEEP_BETWEEN_SYMBOL = 1.0     # seconds between symbols
BASE_SLEEP_MS        = 800     # base backoff for 429
MAX_RETRIES          = 8
# DB comparison tolerances
ABS_EPS              = 0.01    # ₹0.01 absolute tolerance
REL_EPS              = 0.001   # 0.10% relative tolerance

# -------- Env --------
load_dotenv(".env"); load_dotenv("infra/.env")
KITE_API_KEY = os.getenv("KITE_API_KEY")
TOKEN_FILE   = os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")
TOKENS_CSV   = os.getenv("ZERODHA_TOKENS_CSV","configs/tokens.csv")

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

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

# -------- Zerodha helpers --------
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

# -------- Time helpers --------
IST = timezone(timedelta(hours=5, minutes=30))

def ist_window_utc(day: date):
    s = datetime.combine(day, datetime.min.time(), IST).replace(hour=9, minute=15)
    e = datetime.combine(day, datetime.min.time(), IST).replace(hour=15, minute=30)
    return s.astimezone(timezone.utc), e.astimezone(timezone.utc)

def span_ist_utc(start_day: date, end_day: date):
    s_utc, _ = ist_window_utc(start_day)
    _, e_utc = ist_window_utc(end_day)
    return s_utc, e_utc

def _to_minute(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc)
    return ts.replace(second=0, microsecond=0)

# -------- Rate-limit backoff --------
def _rate_limited(e: Exception) -> bool:
    msg = str(e).lower()
    return ("too many request" in msg) or ("429" in msg) or ("rate" in msg)

def _backoff_sleep(base_ms: int, attempt: int):
    time.sleep((base_ms/1000.0) * (2 ** attempt) + random.uniform(0, 0.25))

# -------- Day-bounded interpolation of ALL intraday gaps --------
def _lin(a: float, b: float, t: float) -> float:
    return a + (b - a) * t

def _fill_day_interp_all(
    candles: List[dict], start_day: date, end_day: date
) -> List[Tuple[datetime, float,float,float,float,int,str]]:
    """
    For each day in [start_day, end_day], inside 09:15–15:30 IST:
      - Collect anchor bars (real Zerodha minutes).
      - If <2 anchors, skip day (no extrapolation).
      - Emit anchors as 'Zerodha'.
      - Interpolate every missing minute between consecutive anchors as 'SynthAvg'.
      - Never fill across days.
    """
    by_ts: Dict[datetime, dict] = {}
    for c in candles or []:
        by_ts[_to_minute(c["date"])] = c

    out: List[Tuple[datetime, float,float,float,float,int,str]] = []

    cur = start_day
    while cur <= end_day:
        d_start_utc, d_end_utc = ist_window_utc(cur)
        anchors = sorted(ts for ts in by_ts.keys() if d_start_utc <= ts <= d_end_utc)
        if len(anchors) < 2:
            cur += timedelta(days=1); continue

        def add_real(ts: datetime, c: dict):
            out.append((ts,
                        float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]),
                        int(c.get("volume") or 0), "Zerodha"))

        add_real(anchors[0], by_ts[anchors[0]])
        for i in range(len(anchors)-1):
            a, b = anchors[i], anchors[i+1]
            gap_missing = int((b - a).total_seconds() // 60) - 1
            if gap_missing > 0:
                ca, cb = by_ts[a], by_ts[b]
                steps = gap_missing + 1
                for k in range(1, gap_missing+1):
                    t = k / steps
                    o = _lin(float(ca["open"]),  float(cb["open"]),  t)
                    h = _lin(float(ca["high"]),  float(cb["high"]),  t)
                    l = _lin(float(ca["low"]),   float(cb["low"]),   t)
                    c = _lin(float(ca["close"]), float(cb["close"]), t)
                    v = int(round(_lin(float(ca.get("volume") or 0), float(cb.get("volume") or 0), t)))
                    out.append((a + timedelta(minutes=k), o,h,l,c,v, "SynthAvg"))
            # right anchor
            add_real(b, by_ts[b])

        cur += timedelta(days=1)

    return out

# -------- DB comparison helpers --------
def _within(a: float, b: float) -> bool:
    if a is None or b is None:
        return a == b
    if b == 0:
        return abs(a - b) <= ABS_EPS
    return abs(a - b) <= max(ABS_EPS, REL_EPS * abs(b))

def _same_row(db_row, o,h,l,c,vol) -> bool:
    return (_within(float(db_row["o"]), o) and
            _within(float(db_row["h"]), h) and
            _within(float(db_row["l"]), l) and
            _within(float(db_row["c"]), c) and
            int(db_row["vol"]) == int(vol))

# -------- Main --------
async def main():
    today = date.today()
    start_day = today - timedelta(days=DAYS_BACK)
    end_day   = today - timedelta(days=1)

    kite   = _load_kite()
    tokens = _load_tokens_from_csv()
    pool   = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB,
                                       user=PG_USER, password=PG_PASS)
    async with pool.acquire() as con:
        await con.execute(DDL)

    grand_ins = grand_upd = grand_skip = 0
    grand_real_written = grand_synth_written = 0

    try:
        for tok, sym in tokens:
            from_utc, to_utc = span_ist_utc(start_day, end_day)

            # Fetch once for full 60d; backoff if needed
            attempt = 0
            while True:
                try:
                    raw = kite.historical_data(tok, from_utc, to_utc, "minute",
                                               continuous=False, oi=False) or []
                    break
                except Exception as e:
                    if _rate_limited(e) and attempt < MAX_RETRIES:
                        print(f"[rate-limit] {sym} attempt={attempt+1}/{MAX_RETRIES}: {e}")
                        _backoff_sleep(BASE_SLEEP_MS, attempt); attempt += 1; continue
                    print(f"[error] {sym}: {e}")
                    raw = []; break

            # Build expected minute set (anchors + interpolated)
            rows = _fill_day_interp_all(raw, start_day, end_day)

            # Load ALL existing DB rows in this window for comparison
            async with pool.acquire() as con:
                existing = await con.fetch(
                    "SELECT ts,o,h,l,c,vol,source FROM bars_1m_golden "
                    "WHERE symbol=$1 AND ts >= $2 AND ts <= $3",
                    sym, from_utc, to_utc
                )
            existing_map = { r["ts"].replace(second=0, microsecond=0, tzinfo=timezone.utc): r
                             for r in existing }

            to_write = []
            ins = upd = skip = real_w = synth_w = 0

            for (ts,o,h,l,c,vol,src) in rows:
                db = existing_map.get(ts)
                if db is None:
                    to_write.append((sym, ts, o,h,l,c,vol, src))
                    ins += 1
                    if src == "Zerodha": real_w += 1
                    else:                 synth_w += 1
                else:
                    if _same_row(db, o,h,l,c,vol):
                        skip += 1
                    else:
                        to_write.append((sym, ts, o,h,l,c,vol, src))
                        upd += 1
                        if src == "Zerodha": real_w += 1
                        else:                 synth_w += 1

            # Upsert changed rows
            if to_write:
                async with pool.acquire() as con:
                    await con.executemany(UPSERT_SQL, to_write)

            grand_ins += ins; grand_upd += upd; grand_skip += skip
            grand_real_written  += real_w
            grand_synth_written += synth_w

            print(f"[60d] {sym} window={start_day}→{end_day} "
                  f"expected={len(rows)} write(ins={ins},upd={upd}) skip={skip} "
                  f"writes_real={real_w} writes_synth={synth_w}")

            time.sleep(SLEEP_BETWEEN_SYMBOL)

    finally:
        await pool.close()
        print(f"Done. total writes: ins={grand_ins}, upd={grand_upd}, skip={grand_skip}; "
              f"real_written={grand_real_written}, synth_written={grand_synth_written}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
