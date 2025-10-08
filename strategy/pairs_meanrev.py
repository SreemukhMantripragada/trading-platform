"""
DB-driven pairs mean-reversion backtest (no Kafka).
- Loads pairs + params from configs/pairs.yaml (per-pair timeframe allowed).
- Pulls 1m bars for all symbols from Postgres between start/end.
- Resamples in-process to each pair's TF, aligns legs on closed buckets,
  computes rolling spread z-score (dynamic OLS beta or fixed), and simulates
  entry/exit/stop/timeout with simple notional sizing.
- Writes CSV summary + per-trade ledger; can also upsert to a results table if present.

Env:
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
  COSTS_FILE (default: configs/costs.yaml)

Usage:
  python -m backtest.pairs_backtest_db --pairs configs/pairs.yaml \
    --start 2025-07-16 --end 2025-09-13 --out data/backtests/pairs_bt.csv
"""

import os, sys, csv, math, argparse, yaml, json
import asyncio, asyncpg
from collections import defaultdict, deque
from datetime import datetime, date, time, timedelta, timezone

# ---- helpers -----------------------------------------------------------------

def ist_trading_window_utc(d: date):
    ist = timezone(timedelta(hours=5, minutes=30))
    s = datetime.combine(d, time(9,15), ist).astimezone(timezone.utc)
    e = datetime.combine(d, time(15,30), ist).astimezone(timezone.utc)
    return s, e

def daterange_utc(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur = cur + timedelta(days=1)

def load_yaml(path, default=None):
    if not os.path.exists(path):
        return {} if default is None else default
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

# ---- rolling stats + resampler ----------------------------------------------

class Rolling:
    """Keeps rolling windows of x, y and spread; supports OLS beta and z-score."""
    def __init__(self, maxlen:int):
        self.maxlen = maxlen
        self.x = deque(maxlen=maxlen)
        self.y = deque(maxlen=maxlen)
        self.s = deque(maxlen=maxlen)  # spread = y - beta*x (beta evaluated per push)

    def push(self, xv: float, yv: float, beta: float):
        self.x.append(float(xv))
        self.y.append(float(yv))
        self.s.append(float(yv) - float(beta) * float(xv))

    def beta_ols(self):
        n = len(self.x)
        if n < 10:  # arbitrary min for stability
            return None
        sx = sy = sxx = sxy = 0.0
        for a, b in zip(self.x, self.y):
            sx += a; sy += b; sxx += a*a; sxy += a*b
        den = (n * sxx - sx * sx)
        if abs(den) < 1e-12:
            return None
        return (n * sxy - sx * sy) / den

    def zscore(self):
        n = len(self.s)
        if n < 2: return None
        m = sum(self.s)/n
        var = sum((v - m)*(v - m) for v in self.s)/(n-1)
        sd = math.sqrt(var) if var > 0 else 0.0
        if sd == 0.0: return None
        # Last spread vs mean/sd
        z = (self.s[-1] - m) / sd
        return float(z), float(m), float(sd)

class TFResampler:
    """Tumbling window resampler for close prices from 1m to TF minutes."""
    def __init__(self, tf_minutes:int):
        self.step = tf_minutes * 60
        self.bucket = None     # epoch sec start
        self.last_close = None

    def update(self, ts_epoch: int, close: float):
        b = ts_epoch - (ts_epoch % self.step)
        if self.bucket is None:
            self.bucket = b
            self.last_close = close
            return None
        if b != self.bucket:
            out_ts = self.bucket
            out_px = self.last_close
            self.bucket = b
            self.last_close = close
            return (out_ts, out_px)
        self.last_close = close
        return None

# ---- backtest core -----------------------------------------------------------

async def fetch_1m(pool, symbols, start_utc, end_utc):
    """
    Returns dict symbol -> list[(epoch_sec, close)]
    Pulls from bars_1m between UTC timestamps inclusive.
    """
    sql = """
      SELECT symbol, EXTRACT(EPOCH FROM ts)::bigint AS es, c
      FROM bars_1m
      WHERE symbol = ANY($1::text[]) AND ts >= $2 AND ts <= $3
      ORDER BY symbol, ts;
    """
    out = {s: [] for s in symbols}
    async with pool.acquire() as con:
        rows = await con.fetch(sql, symbols, start_utc, end_utc)
    for r in rows:
        out[r["symbol"]].append((int(r["es"]), float(r["c"])))
    return out

def apply_costs_and_slippage(qty:int, px_entry:float, px_exit:float, cost_cfg:dict):
    """
    Very simple cost model for two-leg roundtrip *per leg*:
      - slippage_bps applied on both entry and exit prices in adverse direction
      - fee_per_order flat (INR) per order
    Returns pnl_leg (INR).
    """
    sl_bps = float(cost_cfg.get("slippage_bps", 2.0))  # 2 bps default
    fee = float(cost_cfg.get("fee_per_order", 5.0))    # ₹5 per order default

    # Effective traded prices with slippage (worse by sl_bps)
    px_in = px_entry * (1.0 + sl_bps/10000.0)
    px_out = px_exit  * (1.0 - sl_bps/10000.0)

    # PnL = (sell - buy) * qty - fees (2 orders)
    # Use sign by scenario; caller chooses BUY/SELL mapping.
    gross = (px_out - px_in) * qty
    net = gross - 2.0*fee
    return net

def simulate_pair(key, pair_cfg, series_x, series_y, tf_minutes, start_utc, end_utc, costs_cfg):
    """
    series_x, series_y: lists of (ts_epoch_sec, close) at 1m frequency (possibly sparse across session gaps)
    Resample to tf, align buckets, compute z, simulate entries/exits.
    Returns: (summary_dict, trades_list)
    """
    look = int(pair_cfg["lookback"])
    beta_mode = pair_cfg.get("hedge_mode","dynamic")
    fixed_beta = pair_cfg.get("fixed_beta", 1.0)
    entry_z = float(pair_cfg["entry_z"])
    exit_z  = float(pair_cfg["exit_z"])
    stop_z  = float(pair_cfg["stop_z"])
    max_hold_min = int(pair_cfg.get("max_hold_min", 240))
    notional = float(pair_cfg.get("notional_per_leg", 5000.0))

    # Resamplers
    rx = TFResampler(tf_minutes); ry = TFResampler(tf_minutes)
    # Iterate merged timeline (two pointers)
    i=j=0; nx=len(series_x); ny=len(series_y)
    roll = Rolling(maxlen=max(look, pair_cfg.get("beta_lookback", look)))
    beta = None if beta_mode=="dynamic" else float(fixed_beta)

    open_pos = None  # {"side": "LONGSPREAD"/"SHORTSPREAD", "since": ts}
    trades = []
    n_bars=0

    def sized_qty(px, b):
        # crude: notional / price; for beta on Y leg scale to keep hedge roughly neutral
        return max(1, int(notional/max(px,1.0))), max(1, int((notional/max(px,1.0))/max(abs(b),1.0)))

    # walk through both symbol streams by ts, feeding resamplers
    pend = {}  # bucket_ts -> {"x":close, "y":close}
    while i<nx or j<ny:
        tx = series_x[i][0] if i<nx else 1<<62
        ty = series_y[j][0] if j<ny else 1<<62
        if tx <= ty:
            out = rx.update(series_x[i][0], series_x[i][1]); i+=1
            if out:
                t, px = out
                d = pend.setdefault(t, {})
                d["x"] = px
        else:
            out = ry.update(series_y[j][0], series_y[j][1]); j+=1
            if out:
                t, py = out
                d = pend.setdefault(t, {})
                d["y"] = py

        # process any complete buckets in chronological order
        ready_ts = sorted([t for t, d in pend.items() if "x" in d and "y" in d])
        for t in ready_ts:
            if t < int(start_utc.timestamp()) or t > int(end_utc.timestamp()):
                del pend[t]; continue
            n_bars += 1
            x = pend[t]["x"]; y = pend[t]["y"]
            # update beta/spread
            cur_beta = beta if beta is not None else 1.0
            roll.push(x, y, cur_beta)
            if beta is None:  # dynamic
                b = roll.beta_ols()
                if b is not None: cur_beta = b
            zmu = roll.zscore()
            if zmu is None:
                del pend[t]; continue
            z, mu, sd = zmu

            # rules
            if open_pos is None and abs(z) >= entry_z:
                # enter
                side = "SHORTSPREAD" if z > 0 else "LONGSPREAD"
                # decide leg quantities using cur_beta and price ref
                qx, qy = sized_qty(x, cur_beta)
                trades.append({
                    "ts": t, "action": "OPEN", "side": side, "z": z, "beta": cur_beta,
                    "x_px": x, "y_px": y, "qx": qx, "qy": qy
                })
                open_pos = {"side": side, "since": t, "qx": qx, "qy": qy, "x_open": x, "y_open": y, "beta": cur_beta}

            elif open_pos is not None:
                held_min = (t - open_pos["since"]) / 60
                should_exit = abs(z) <= exit_z
                hit_stop    = abs(z) >= stop_z
                timeout     = held_min >= max_hold_min
                if should_exit or hit_stop or timeout:
                    # exit
                    side = open_pos["side"]; qx=open_pos["qx"]; qy=open_pos["qy"]
                    # price mapping:
                    #  LONGSPREAD = long y, short x  (enter: BUY Y, SELL X) → exit: SELL Y, BUY X
                    # SHORTSPREAD = short y, long x (enter: SELL Y, BUY X) → exit: BUY Y, SELL X
                    # PnL per leg with costs/slippage
                    if side=="LONGSPREAD":
                        # y: buy @y_open → sell @y,  x: sell @x_open → buy @x
                        pnl_y = apply_costs_and_slippage(qy, open_pos["y_open"], y, costs_cfg)
                        pnl_x = -apply_costs_and_slippage(qx, open_pos["x_open"], x, costs_cfg)  # short leg: reverse sign
                    else:
                        # y: sell then buy; x: buy then sell
                        pnl_y = -apply_costs_and_slippage(qy, open_pos["y_open"], y, costs_cfg)
                        pnl_x = apply_costs_and_slippage(qx, open_pos["x_open"], x, costs_cfg)
                    pnl = pnl_x + pnl_y
                    trades.append({
                        "ts": t, "action": ("STOP" if hit_stop else ("TIMEOUT" if timeout else "CLOSE")),
                        "side": side, "z": z, "beta": open_pos["beta"],
                        "x_px": x, "y_px": y, "qx": qx, "qy": qy, "pnl": pnl
                    })
                    open_pos = None
            del pend[t]

    # If still open, flat at last seen bucket price (conservative)
    if open_pos is not None and trades:
        last_t = trades[-1]["ts"]; x = trades[-1]["x_px"]; y = trades[-1]["y_px"]
        side = open_pos["side"]; qx=open_pos["qx"]; qy=open_pos["qy"]
        if side=="LONGSPREAD":
            pnl_y = apply_costs_and_slippage(qy, open_pos["y_open"], y, costs_cfg)
            pnl_x = -apply_costs_and_slippage(qx, open_pos["x_open"], x, costs_cfg)
        else:
            pnl_y = -apply_costs_and_slippage(qy, open_pos["y_open"], y, costs_cfg)
            pnl_x = apply_costs_and_slippage(qx, open_pos["x_open"], x, costs_cfg)
        pnl = pnl_x + pnl_y
        trades.append({"ts": last_t, "action": "FORCE_CLOSE", "side": side, "x_px": x, "y_px": y, "qx": qx, "qy": qy, "pnl": pnl})

    # summary
    closed = [t for t in trades if t["action"] in ("CLOSE","STOP","TIMEOUT","FORCE_CLOSE")]
    pnl_sum = sum(t.get("pnl",0.0) for t in closed)
    n_tr = len(closed)
    win = sum(1 for t in closed if t.get("pnl",0.0) > 0)
    wr = (win/n_tr) if n_tr>0 else 0.0
    avg = (pnl_sum/n_tr) if n_tr>0 else 0.0

    return {
        "pair": key, "tf_min": tf_minutes, "n_bars": n_bars, "trades": n_tr,
        "win_rate": wr, "pnl_total": pnl_sum, "pnl_avg": avg
    }, trades

# ---- main --------------------------------------------------------------------

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", default="configs/pairs.yaml")
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD
    ap.add_argument("--out", default="data/backtests/pairs_bt.csv")
    args = ap.parse_args()

    pairs_cfg = load_yaml(args.pairs)
    costs_cfg = load_yaml(os.getenv("COSTS_FILE","configs/costs.yaml"))

    pairs = pairs_cfg.get("pairs", [])
    if not pairs:
        print(f"No pairs in {args.pairs}")
        sys.exit(1)

    # Collect all symbols
    syms = sorted(set([p["leg_x"] for p in pairs] + [p["leg_y"] for p in pairs]))
    start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_d   = datetime.strptime(args.end, "%Y-%m-%d").date()

    # Env DB
    PG_HOST=os.getenv("POSTGRES_HOST","localhost")
    PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
    PG_DB=os.getenv("POSTGRES_DB","trading")
    PG_USER=os.getenv("POSTGRES_USER","trader")
    PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS, min_size=1, max_size=4)

    try:
        # Pull all days into memory once to reduce roundtrips:
        s0_utc, eN_utc = ist_trading_window_utc(start_d)[0], ist_trading_window_utc(end_d)[1]
        series_all = await fetch_1m(pool, syms, s0_utc, eN_utc)

        # For each pair: slice needed series directly
        summaries = []
        ledger = []
        for p in pairs:
            key = f'{p["leg_x"]}|{p["leg_y"]}|{p.get("tf","1m")}'
            tfm = {"1m":1,"3m":3,"5m":5,"15m":15}.get(p.get("tf","1m"),1)
            sx = series_all.get(p["leg_x"], [])
            sy = series_all.get(p["leg_y"], [])
            # simulate across entire window [start_d..end_d] within daily trading sessions
            # We already limited fetch to the broad range; just feed as-is.
            summ, tr = simulate_pair(key, p, sx, sy, tfm, s0_utc, eN_utc, costs_cfg)
            summaries.append(summ)
            for t in tr:
                row = {"pair": key, **t}
                # human ts
                row["ts_iso"] = datetime.utcfromtimestamp(row["ts"]).isoformat()
                ledger.append(row)

        # Write CSV (summary + trades as separate files)
        out_summary = args.out
        out_trades  = args.out.replace(".csv","_trades.csv")

        os.makedirs(os.path.dirname(out_summary) or ".", exist_ok=True)
        with open(out_summary, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(summaries[0].keys()))
            w.writeheader(); w.writerows(summaries)
        with open(out_trades, "w", newline="") as f:
            if ledger:
                w = csv.DictWriter(f, fieldnames=list(ledger[0].keys()))
                w.writeheader(); w.writerows(ledger)
            else:
                f.write("")  # empty

        print(f"[pairs-bt] wrote summary -> {out_summary} ; trades -> {out_trades}")
        # Optional: attempt DB insert if table exists
        async with pool.acquire() as con:
            has_tbl = await con.fetchval("""
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema='public' AND table_name='backtest_pairs_results'
                );
            """)
            if has_tbl:
                await con.executemany("""
                  INSERT INTO backtest_pairs_results
                  (pair, tf_min, n_bars, trades, win_rate, pnl_total, pnl_avg, run_ts)
                  VALUES($1,$2,$3,$4,$5,$6,$7, now())
                """, [(s["pair"], s["tf_min"], s["n_bars"], s["trades"], s["win_rate"], s["pnl_total"], s["pnl_avg"]) for s in summaries])

    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
