"""
DB-driven pairs mean-reversion backtest (no Kafka, no date args).
Always runs on the last 60 calendar days of IST sessions using bars_1m.
Writes:
  data/backtests/pairs_bt.csv
  data/backtests/pairs_bt_trades.csv
"""

import os, sys, csv, math, yaml, asyncio, asyncpg
from collections import deque
from datetime import datetime, date, time, timedelta, timezone

# ----------------- helpers -----------------
def load_yaml(path, default=None):
    if not os.path.exists(path):
        return {} if default is None else default
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

IST = timezone(timedelta(hours=5, minutes=30))

def ist_session_window_for_day(d: date):
    s_ist = datetime.combine(d, time(9, 15), IST)
    e_ist = datetime.combine(d, time(15, 30), IST)
    return s_ist.astimezone(timezone.utc), e_ist.astimezone(timezone.utc)

def last_120_days_session_window_utc(now_utc=None):
    now_utc = now_utc or datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)
    end_day = now_ist.date()
    if now_ist.time() < time(15, 25):
        end_day = end_day - timedelta(days=1)
    start_day = end_day - timedelta(days=120)
    s_utc, _ = ist_session_window_for_day(start_day)
    _, e_utc = ist_session_window_for_day(end_day)
    return s_utc, e_utc, start_day, end_day

class TFResampler:
    def __init__(self, tf_minutes:int):
        self.step = tf_minutes * 60
        self.bucket = None
        self.last_close = None
    def update(self, ts_epoch: int, close: float):
        b = ts_epoch - (ts_epoch % self.step)
        if self.bucket is None:
            self.bucket = b; self.last_close = close
            return None
        if b != self.bucket:
            out = (self.bucket, self.last_close)
            self.bucket = b; self.last_close = close
            return out
        self.last_close = close
        return None

async def fetch_1m(pool, symbols, start_utc, end_utc):
    sql = """
      SELECT symbol, EXTRACT(EPOCH FROM ts)::bigint AS es, c
      FROM bars_1m_golden
      WHERE symbol = ANY($1::text[]) AND ts >= $2 AND ts <= $3
      ORDER BY symbol, ts;
    """
    out = {s: [] for s in symbols}
    async with pool.acquire() as con:
        rows = await con.fetch(sql, symbols, start_utc, end_utc)
    for r in rows:
        out[r["symbol"]].append((int(r["es"]), float(r["c"])))
    return out

def apply_costs_and_slippage(qty:int, px_in:float, px_out:float, cost_cfg:dict):
    sl_bps = float(cost_cfg.get("slippage_bps", 2.0))
    fee    = float(cost_cfg.get("fee_per_order", 5.0))
    eff_in  = px_in  * (1.0 + sl_bps/10000.0)
    eff_out = px_out * (1.0 - sl_bps/10000.0)
    gross = (eff_out - eff_in) * qty
    return gross - 2.0 * fee

def simulate_pair(key, pair_cfg, series_x, series_y, tf_minutes, start_utc, end_utc, costs_cfg):
    look        = int(pair_cfg.get("lookback", 60))
    beta_mode   = pair_cfg.get("hedge_mode", "dynamic")  # "dynamic" or "fixed"
    fixed_beta  = float(pair_cfg.get("fixed_beta", 1.0))
    entry_z     = float(pair_cfg.get("entry_z", 2.0))
    exit_z      = float(pair_cfg.get("exit_z", 0.5))
    stop_z      = float(pair_cfg.get("stop_z", 4.0))
    max_hold_min= int(pair_cfg.get("max_hold_min", 240))
    notional    = float(pair_cfg.get("notional_per_leg", 50000))

    rx = TFResampler(tf_minutes); ry = TFResampler(tf_minutes)
    i=j=0; nx=len(series_x); ny=len(series_y)
    roll = deque(maxlen=max(look, int(pair_cfg.get("beta_lookback", look))))
    roll_x = deque(maxlen=max(look, int(pair_cfg.get("beta_lookback", look))))
    roll_y = deque(maxlen=max(look, int(pair_cfg.get("beta_lookback", look))))
    beta = None if beta_mode == "dynamic" else fixed_beta

    def push_and_stats(x, y, cur_beta):
        roll_x.append(x); roll_y.append(y); roll.append(y - cur_beta*x)
        n = len(roll)
        if n < 2: return None, None, None
        m = sum(roll)/n
        var = sum((v-m)*(v-m) for v in roll)/(n-1)
        sd = math.sqrt(var) if var>0 else 0.0
        if sd == 0.0: return None, m, sd
        return (roll[-1] - m)/sd, m, sd

    def beta_ols():
        n = len(roll_x)
        if n < 10: return None
        sx=sy=sxx=sxy=0.0
        for a,b in zip(roll_x, roll_y):
            sx += a; sy += b; sxx += a*a; sxy += a*b
        den = n*sxx - sx*sx
        if abs(den) < 1e-12: return None
        return (n*sxy - sx*sy)/den

    def sized_qty(px, b):
        qx = max(1, int(notional/max(px,1.0)))
        qy = max(1, int((notional/max(px,1.0))/max(abs(b),1.0)))
        return qx, qy

    open_pos = None
    trades = []
    n_bars = 0
    pend = {}

    while i<nx or j<ny:
        tx = series_x[i][0] if i<nx else 1<<62
        ty = series_y[j][0] if j<ny else 1<<62
        if tx <= ty:
            out = rx.update(series_x[i][0], series_x[i][1]); i+=1
            if out: pend.setdefault(out[0], {})["x"] = out[1]
        else:
            out = ry.update(series_y[j][0], series_y[j][1]); j+=1
            if out: pend.setdefault(out[0], {})["y"] = out[1]

        ready = sorted([t for t, d in pend.items() if "x" in d and "y" in d])
        for t in ready:
            if t < int(start_utc.timestamp()) or t > int(end_utc.timestamp()):
                del pend[t]; continue
            n_bars += 1
            x = pend[t]["x"]; y = pend[t]["y"]
            cur_beta = beta if beta is not None else 1.0
            roll_x.append(x); roll_y.append(y)
            if beta is None:
                b = beta_ols()
                if b is not None: cur_beta = b
            z, m, sd = push_and_stats(x, y, cur_beta)
            if z is None:
                del pend[t]; continue

            if open_pos is None and abs(z) >= entry_z:
                side = "SHORTSPREAD" if z > 0 else "LONGSPREAD"
                qx, qy = sized_qty(x, cur_beta)
                trades.append({"ts": t, "action": "OPEN", "side": side, "beta": cur_beta,
                               "z": float(z), "x_px": x, "y_px": y, "qx": qx, "qy": qy})
                open_pos = {"since": t, "side": side, "beta": cur_beta, "qx": qx, "qy": qy,
                            "x_open": x, "y_open": y}
            elif open_pos is not None:
                held_min = (t - open_pos["since"]) / 60
                should_exit = abs(z) <= exit_z
                hit_stop    = abs(z) >= stop_z
                timeout     = held_min >= max_hold_min
                if should_exit or hit_stop or timeout:
                    side = open_pos["side"]; qx=open_pos["qx"]; qy=open_pos["qy"]
                    if side == "LONGSPREAD":
                        pnl_y = apply_costs_and_slippage(qy, open_pos["y_open"], y, costs_cfg)
                        pnl_x = -apply_costs_and_slippage(qx, open_pos["x_open"], x, costs_cfg)
                    else:
                        pnl_y = -apply_costs_and_slippage(qy, open_pos["y_open"], y, costs_cfg)
                        pnl_x =  apply_costs_and_slippage(qx, open_pos["x_open"], x, costs_cfg)
                    pnl = pnl_x + pnl_y
                    trades.append({"ts": t, "action": ("STOP" if hit_stop else ("TIMEOUT" if timeout else "CLOSE")),
                                   "side": side, "beta": open_pos["beta"], "z": float(z),
                                   "x_px": x, "y_px": y, "qx": qx, "qy": qy, "pnl": float(pnl)})
                    open_pos = None
            del pend[t]

    if open_pos is not None and trades:
        last_t = trades[-1]["ts"]; x = trades[-1]["x_px"]; y = trades[-1]["y_px"]
        side = open_pos["side"]; qx=open_pos["qx"]; qy=open_pos["qy"]
        if side == "LONGSPREAD":
            pnl_y = apply_costs_and_slippage(qy, open_pos["y_open"], y, costs_cfg)
            pnl_x = -apply_costs_and_slippage(qx, open_pos["x_open"], x, costs_cfg)
        else:
            pnl_y = -apply_costs_and_slippage(qy, open_pos["y_open"], y, costs_cfg)
            pnl_x =  apply_costs_and_slippage(qx, open_pos["x_open"], x, costs_cfg)
        pnl = pnl_x + pnl_y
        trades.append({"ts": last_t, "action": "FORCE_CLOSE", "side": side,
                       "x_px": x, "y_px": y, "qx": qx, "qy": qy, "pnl": float(pnl)})

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

# ----------------- main --------------------
async def main():
    pairs_cfg = load_yaml("configs/pairs.yaml")
    costs_cfg = load_yaml(os.getenv("COSTS_FILE","configs/costs.yaml"))
    pairs = pairs_cfg.get("pairs", [])
    if not pairs:
        print("No pairs defined in configs/pairs.yaml"); sys.exit(1)

    s_utc, e_utc, s_day, e_day = last_120_days_session_window_utc()
    print(f"[pairs-bt] window IST-days: {s_day} → {e_day}")

    syms = sorted({p["leg_x"] for p in pairs} | {p["leg_y"] for p in pairs})

    PG_HOST=os.getenv("POSTGRES_HOST","localhost")
    PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
    PG_DB=os.getenv("POSTGRES_DB","trading")
    PG_USER=os.getenv("POSTGRES_USER","trader")
    PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS, min_size=1, max_size=4)
    try:
        series_all = await fetch_1m(pool, syms, s_utc, e_utc)

        summaries = []
        ledger = []
        for idx, p in enumerate(pairs, 1):
            key = f'{p["leg_x"]}|{p["leg_y"]}|{p.get("tf","1m")}'
            tfm = {"1m":1, "3m":3, "5m":5, "15m":15}.get(p.get("tf","1m"), 1)
            sx = series_all.get(p["leg_x"], [])
            sy = series_all.get(p["leg_y"], [])
            summ, tr = simulate_pair(key, p, sx, sy, tfm, s_utc, e_utc, costs_cfg)
            summaries.append(summ)
            for t in tr:
                t_iso = datetime.fromtimestamp(t["ts"], timezone.utc).isoformat()
                row = {"pair": key, **t, "ts_iso": t_iso}
                ledger.append(row)
            if idx % 5 == 0 or idx == len(pairs):
                print(f"[pairs-bt] processed {idx}/{len(pairs)} pairs")

        os.makedirs("data/backtests", exist_ok=True)
        out_summary = "data/backtests/pairs_bt.csv"
        out_trades  = "data/backtests/pairs_bt_trades.csv"

        with open(out_summary, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(summaries[0].keys()))
            w.writeheader(); w.writerows(summaries)

        with open(out_trades, "w", newline="") as f:
            if ledger:
                # Use UNION of keys so rows with/without 'pnl' etc. write cleanly
                fieldnames = sorted({k for r in ledger for k in r.keys()})
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                for r in ledger:
                    w.writerow({k: r.get(k, "") for k in fieldnames})

        print(f"[pairs-bt] wrote summary -> {out_summary}")
        print(f"[pairs-bt] wrote trades  -> {out_trades}")

        async with pool.acquire() as con:
            has_tbl = await con.fetchval("""
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema='public' AND table_name='backtest_pairs_results'
                );
            """)
            if has_tbl and summaries:
                await con.executemany("""
                  INSERT INTO backtest_pairs_results
                  (pair, tf_min, n_bars, trades, win_rate, pnl_total, pnl_avg, run_ts)
                  VALUES($1,$2,$3,$4,$5,$6,$7, now())
                """, [(s["pair"], s["tf_min"], s["n_bars"], s["trades"], s["win_rate"], s["pnl_total"], s["pnl_avg"]) for s in summaries])

    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
