"""

This script performs a mean-reversion backtest for trading pairs using historical 1-minute bar data stored in a PostgreSQL database. It simulates trading strategies for all pairs defined in a YAML configuration, applies transaction costs and slippage, and evaluates performance metrics such as PnL, win rate, and drawdown. The results are scored, filtered, and saved to CSV files and the database. The script supports parallel execution using multiprocessing and generates a YAML file with top picks for the next trading day.

Key Features:
- Loads pairs and strategy parameters from YAML configuration files.
- Fetches historical 1-minute bar data for all symbols from PostgreSQL.
- Simulates mean-reversion trading for each pair, with configurable lookback, entry/exit/stop thresholds, and risk management.
- Applies transaction costs and slippage to all trades.
- Supports both fixed and dynamic (OLS-estimated) hedge ratios (beta).
- Aggregates and scores results using customizable gates and weights.
- Outputs results to CSV files and updates a database table for backtest results.
- Generates a YAML file with top picks for the next trading day, respecting symbol and risk constraints.
- Supports parallel execution with configurable worker processes.

Environment Variables:
- POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD: PostgreSQL connection parameters.
- COSTS_FILE: Path to transaction costs configuration YAML.
- PAIRS_BT_WORKERS: Number of worker processes for parallel execution.
- PAIRS_BT_CTX: Multiprocessing context ("fork", "spawn", etc.).

Command-line Arguments:
- --workers: Number of worker processes (<=0 for auto-detect).

Dependencies:
- asyncpg, yaml, csv, json, math, multiprocessing, argparse, and custom modules from backtest package.

Outputs:
- data/backtests/pairs_bt.csv: Summary of all backtest results.
- data/backtests/pairs_bt_trades.csv: All trade records for all pairs.
- data/backtests/pairs_bt_scored.csv: Scored and ranked results.
- YAML file with next-day picks (path configurable).
- Optionally updates the backtest_pairs_results table in the database.
DB-driven pairs mean-reversion backtest (no Kafka, no date args).
Always runs on the last 60 calendar days of IST sessions using bars_1m.
Writes:
  data/backtests/pairs_bt.csv
  data/backtests/pairs_bt_trades.csv
"""

import os, sys, csv, math, json, yaml, asyncio, asyncpg, argparse
import multiprocessing as mp
from collections import deque
from datetime import datetime, date, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from backtest.scorer import (
    DEFAULT_GATES,
    DEFAULT_WEIGHTS,
    apply_constraints,
    score_rows,
)

from backtest.persistence import get_latest_run_id, save_results, write_next_day_yaml

# ----------------- helpers -----------------
def load_yaml(path, default=None):
    if not os.path.exists(path):
        return {} if default is None else default
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

IST = timezone(timedelta(hours=5, minutes=30))

TF_LOOKUP = {"1m": 1, "3m": 3, "5m": 5, "15m": 15}

def _parse_flatten_cutoff(val: Optional[str]) -> Optional[int]:
    if not val:
        return None
    try:
        hh, mm = val.split(":")
        hh_i = max(0, min(23, int(hh)))
        mm_i = max(0, min(59, int(mm)))
        return hh_i * 60 + mm_i
    except Exception:
        return None

def _minutes_ist(ts_epoch: int) -> int:
    lt = datetime.fromtimestamp(ts_epoch, timezone.utc).astimezone(IST).time()
    return lt.hour * 60 + lt.minute

def _normalize_dd_thresholds(cfg: Any) -> List[Tuple[str, float]]:
    default = [("LOW", 50_000.0), ("MED", 100_000.0)]
    if cfg is None:
        return default
    items: List[Tuple[str, float]] = []
    is_dict = isinstance(cfg, dict)
    source = cfg.items() if is_dict else cfg
    try:
        iterator = list(source)
    except Exception:
        return default
    for entry in iterator:
        if is_dict:
            bucket, val = entry
        elif isinstance(entry, (list, tuple)) and len(entry) == 2:
            bucket, val = entry
        else:
            continue
        try:
            bucket_str = str(bucket)
            val_float = float(val)
        except Exception:
            continue
        items.append((bucket_str, val_float))
    items.sort(key=lambda x: x[1])
    return items or default

def _bucket_for_dd(dd: Any, thresholds: List[Tuple[str, float]]) -> str:
    if not thresholds:
        return "MED"
    try:
        dd_mag = abs(float(dd or 0.0))
    except Exception:
        dd_mag = 0.0
    for bucket, limit in thresholds:
        try:
            lim = float(limit)
        except Exception:
            continue
        if dd_mag <= lim:
            return bucket
    return thresholds[-1][0]

def _ensure_params_dict(params: Any) -> Dict[str, Any]:
    if params is None:
        return {}
    if isinstance(params, dict):
        return dict(params)
    if isinstance(params, str):
        try:
            parsed = json.loads(params)
            if isinstance(parsed, dict):
                return parsed
            return {}
        except Exception:
            return {}
    if hasattr(params, "items"):
        try:
            return dict(params)
        except Exception:
            return {}
    return {}

def _win_rate_percent(row: Dict[str, Any]) -> float:
    val = row.get("win_rate")
    if val is None:
        val = row.get("win_rate_pct")
    try:
        wr = float(val)
    except Exception:
        return 0.0
    if wr <= 1.0:
        wr *= 100.0
    return wr

def _profit_per_trade_value(row: Dict[str, Any]) -> float:
    ppt = row.get("profit_per_trade")
    if ppt is None:
        ppt = row.get("pnl_avg")
        if ppt is None:
            try:
                pnl = float(row.get("pnl_total") or 0.0)
                n = int(row.get("n_trades") or row.get("trades") or 0)
            except Exception:
                return 0.0
            if n > 0:
                return pnl / n
            return 0.0
    try:
        return float(ppt)
    except Exception:
        return 0.0

def _avg_hold_minutes(row: Dict[str, Any]) -> float:
    val = row.get("avg_hold_min")
    if val is None:
        val = row.get("avg_hold")
    try:
        return float(val)
    except Exception:
        return 0.0

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

def simulate_pair(
    key: str,
    pair_cfg: Dict[str, Any],
    series_x: List[Tuple[int, float]],
    series_y: List[Tuple[int, float]],
    tf_minutes: int,
    start_utc: datetime,
    end_utc: datetime,
    costs_cfg: Dict[str, Any],
    flatten_cutoff_min: Optional[int] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    look        = int(pair_cfg.get("lookback", 120))
    beta_mode   = pair_cfg.get("hedge_mode", "dynamic")  # "dynamic" or "fixed"
    fixed_beta  = float(pair_cfg.get("fixed_beta", 1.0))
    entry_z     = float(pair_cfg.get("entry_z", 2.0))
    exit_z      = float(pair_cfg.get("exit_z", 0.5))
    stop_z      = float(pair_cfg.get("stop_z", 4.0))
    max_hold_min= int(pair_cfg.get("max_hold_min", 240))
    notional    = float(pair_cfg.get("notional_per_leg", 250000))

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

    open_pos: Optional[Dict[str, Any]] = None
    trades: List[Dict[str, Any]] = []
    n_bars = 0
    pend: Dict[int, Dict[str, float]] = {}
    hold_minutes: List[float] = []
    eq = 0.0
    peak_eq = 0.0
    max_dd_val = 0.0
    last_prices: Optional[Tuple[int, float, float]] = None

    def _close_position(t_ts: int, x_px: float, y_px: float, z_val: Optional[float], reason: str):
        nonlocal open_pos, eq, peak_eq, max_dd_val
        if open_pos is None:
            return
        qx = open_pos["qx"]; qy = open_pos["qy"]
        side = open_pos["side"]
        if side == "LONGSPREAD":
            pnl_y = apply_costs_and_slippage(qy, open_pos["y_open"], y_px, costs_cfg)
            pnl_x = -apply_costs_and_slippage(qx, open_pos["x_open"], x_px, costs_cfg)
        else:
            pnl_y = -apply_costs_and_slippage(qy, open_pos["y_open"], y_px, costs_cfg)
            pnl_x =  apply_costs_and_slippage(qx, open_pos["x_open"], x_px, costs_cfg)
        pnl = pnl_x + pnl_y
        eq += pnl
        if eq > peak_eq:
            peak_eq = eq
        dd = peak_eq - eq
        if dd > max_dd_val:
            max_dd_val = dd
        held_min = max(0.0, (t_ts - open_pos["since"]) / 60.0)
        hold_minutes.append(held_min)
        trades.append({
            "ts": t_ts,
            "action": reason,
            "side": side,
            "beta": open_pos["beta"],
            "z": float(z_val) if z_val is not None else None,
            "x_px": x_px,
            "y_px": y_px,
            "qx": qx,
            "qy": qy,
            "pnl": float(pnl),
            "hold_min": float(held_min),
        })
        open_pos = None

    while i < nx or j < ny:
        tx = series_x[i][0] if i < nx else 1 << 62
        ty = series_y[j][0] if j < ny else 1 << 62
        if tx <= ty:
            out = rx.update(series_x[i][0], series_x[i][1])
            i += 1
            if out:
                pend.setdefault(out[0], {})["x"] = out[1]
        else:
            out = ry.update(series_y[j][0], series_y[j][1])
            j += 1
            if out:
                pend.setdefault(out[0], {})["y"] = out[1]

        ready = sorted(t for t, d in pend.items() if "x" in d and "y" in d)
        for t in ready:
            if t < int(start_utc.timestamp()) or t > int(end_utc.timestamp()):
                del pend[t]
                continue
            n_bars += 1
            x = pend[t]["x"]
            y = pend[t]["y"]
            last_prices = (t, x, y)
            cur_beta = beta if beta is not None else 1.0
            roll_x.append(x)
            roll_y.append(y)
            if beta is None:
                b = beta_ols()
                if b is not None:
                    cur_beta = b
            z, _, _ = push_and_stats(x, y, cur_beta)
            if z is None:
                del pend[t]
                continue

            ist_minutes = _minutes_ist(t)
            beyond_flatten = (flatten_cutoff_min is not None) and (ist_minutes >= flatten_cutoff_min)

            if open_pos is None:
                if not beyond_flatten and abs(z) >= entry_z:
                    side = "SHORTSPREAD" if z > 0 else "LONGSPREAD"
                    qx, qy = sized_qty(x, cur_beta)
                    trades.append({
                        "ts": t,
                        "action": "OPEN",
                        "side": side,
                        "beta": cur_beta,
                        "z": float(z),
                        "x_px": x,
                        "y_px": y,
                        "qx": qx,
                        "qy": qy,
                    })
                    open_pos = {
                        "since": t,
                        "side": side,
                        "beta": cur_beta,
                        "qx": qx,
                        "qy": qy,
                        "x_open": x,
                        "y_open": y,
                        "entry_z": float(z),
                        "last_z": float(z),
                    }
                del pend[t]
                continue

            # Position management
            open_pos["last_z"] = float(z)
            held_min = (t - open_pos["since"]) / 60.0
            exit_hit = abs(z) <= exit_z
            stop_hit = abs(z) >= stop_z
            timeout_hit = held_min >= max_hold_min
            if exit_hit or stop_hit or timeout_hit or beyond_flatten:
                if stop_hit:
                    reason = "STOP"
                elif timeout_hit:
                    reason = "TIMEOUT"
                elif beyond_flatten:
                    reason = "FLATTEN"
                else:
                    reason = "CLOSE"
                _close_position(t, x, y, float(z), reason)
            del pend[t]

    if open_pos is not None and last_prices is not None:
        t_last, x_last, y_last = last_prices
        z_last = open_pos.get("last_z")
        _close_position(t_last, x_last, y_last, z_last, "FORCE_CLOSE")

    closed = [t for t in trades if t["action"] in ("CLOSE", "STOP", "TIMEOUT", "FORCE_CLOSE", "FLATTEN")]
    pnl_sum = sum(t.get("pnl", 0.0) for t in closed)
    n_tr = len(closed)
    win = sum(1 for t in closed if t.get("pnl", 0.0) > 0)
    wr = (win / n_tr) if n_tr > 0 else 0.0
    avg = (pnl_sum / n_tr) if n_tr > 0 else 0.0
    avg_hold = (sum(hold_minutes) / len(hold_minutes)) if hold_minutes else 0.0

    summary = {
        "pair": key,
        "tf_min": tf_minutes,
        "tf": f"{tf_minutes}m",
        "n_bars": n_bars,
        "n": n_bars,
        "trades": n_tr,
        "n_trades": n_tr,
        "win_rate": wr,
        "win_rate_pct": wr * 100.0,
        "pnl_total": pnl_sum,
        "pnl_sum": pnl_sum,
        "pnl_avg": avg,
        "profit_per_trade": avg,
        "max_dd": max_dd_val,
        "avg_hold_min": avg_hold,
    }
    return summary, trades


# ----------------- helpers for orchestration --------------------
def _filter_params(pair_cfg: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in pair_cfg.items():
        if k in ("leg_x", "leg_y", "tf", "name", "strategy_name"):
            continue
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v
    return out

def _simulate_pair_core(
    pair_cfg: Dict[str, Any],
    series_lookup: Dict[str, List[Tuple[int, float]]],
    costs_cfg: Dict[str, Any],
    start_utc: datetime,
    end_utc: datetime,
    default_flatten_min: Optional[int],
    default_strategy: str,
):
    local_flatten = _parse_flatten_cutoff(pair_cfg.get("ist_flatten_hhmm")) or default_flatten_min
    tf_key = pair_cfg.get("tf", "1m")
    tfm = TF_LOOKUP.get(tf_key, 1)
    key = f'{pair_cfg["leg_x"]}|{pair_cfg["leg_y"]}|{tf_key}'
    symbol = f'{pair_cfg["leg_x"]}-{pair_cfg["leg_y"]}'

    series_x = series_lookup.get(pair_cfg["leg_x"], [])
    series_y = series_lookup.get(pair_cfg["leg_y"], [])

    summary_core, trades = simulate_pair(
        key,
        pair_cfg,
        series_x,
        series_y,
        tfm,
        start_utc,
        end_utc,
        costs_cfg,
        flatten_cutoff_min=local_flatten,
    )

    params = _filter_params(pair_cfg)
    strategy_name = pair_cfg.get("strategy_name") or default_strategy
    flatten_label = pair_cfg.get("ist_flatten_hhmm")
    if flatten_label is None and local_flatten is not None:
        flatten_label = f"{local_flatten // 60:02d}:{local_flatten % 60:02d}"

    summary = {
        **summary_core,
        "pair": key,
        "symbol": symbol,
        "strategy": strategy_name,
        "timeframe": tfm,
        "params": params,
        "leg_x": pair_cfg["leg_x"],
        "leg_y": pair_cfg["leg_y"],
        "flatten_hhmm": flatten_label,
    }

    summary.setdefault("score", None)
    summary.setdefault("rank", None)
    summary.setdefault("rejected_reason", None)

    ledger_rows: List[Dict[str, Any]] = []
    for trade in trades:
        ts_iso = datetime.fromtimestamp(trade["ts"], timezone.utc).isoformat()
        row = {"pair": key, "symbol": symbol, **trade, "ts_iso": ts_iso}
        ledger_rows.append(row)

    return summary, ledger_rows

_MP_SERIES: Dict[str, List[Tuple[int, float]]] = {}
_MP_COSTS: Dict[str, Any] = {}
_MP_START: Optional[datetime] = None
_MP_END: Optional[datetime] = None
_MP_FLATTEN: Optional[int] = None
_MP_STRATEGY: str = "PAIRS_MEANREV"

def _mp_init(series_lookup, costs_cfg, start_utc, end_utc, flatten_min, strategy_name):
    global _MP_SERIES, _MP_COSTS, _MP_START, _MP_END, _MP_FLATTEN, _MP_STRATEGY
    _MP_SERIES = series_lookup
    _MP_COSTS = costs_cfg
    _MP_START = start_utc
    _MP_END = end_utc
    _MP_FLATTEN = flatten_min
    _MP_STRATEGY = strategy_name

def _mp_worker(args):
    idx, pair_cfg = args
    summary, ledger_rows = _simulate_pair_core(
        pair_cfg,
        _MP_SERIES,
        _MP_COSTS,
        _MP_START,
        _MP_END,
        _MP_FLATTEN,
        _MP_STRATEGY,
    )
    return idx, summary, ledger_rows

def _mp_context():
    preferred = os.getenv("PAIRS_BT_CTX", "fork")
    try:
        return mp.get_context(preferred)
    except ValueError:
        return mp.get_context("spawn")


# ----------------- main --------------------
async def main(workers_override: int | None = None):
    pairs_cfg = load_yaml("configs/pairs.yaml")
    costs_cfg = load_yaml(os.getenv("COSTS_FILE","configs/costs.yaml"))
    pairs = pairs_cfg.get("pairs", [])
    if not pairs:
        print("No pairs defined in configs/pairs.yaml"); sys.exit(1)

    defaults = pairs_cfg.get("defaults", {}) or {}
    default_flatten_str = defaults.get("ist_flatten_hhmm") or "15:15"
    flatten_cutoff_min = _parse_flatten_cutoff(str(default_flatten_str))
    strategy_name = str(defaults.get("strategy_name") or "PAIRS_MEANREV").upper()

    scoring_cfg = pairs_cfg.get("scoring", {}) or {}
    gates = DEFAULT_GATES.copy()
    gates.update(scoring_cfg.get("gates") or {})
    weights = DEFAULT_WEIGHTS.copy()
    weights.update(scoring_cfg.get("weights") or {})
    dd_thresholds = _normalize_dd_thresholds(scoring_cfg.get("dd_buckets") or defaults.get("dd_buckets"))
    risk_per_trade_cfg = scoring_cfg.get("risk_per_trade", defaults.get("risk_per_trade", 0.01))
    try:
        risk_per_trade = float(risk_per_trade_cfg)
    except Exception:
        risk_per_trade = 0.01
    if risk_per_trade <= 0:
        risk_per_trade = 0.01
    topk_cfg = scoring_cfg.get("topk", defaults.get("topk", 20))
    try:
        topk = max(1, int(topk_cfg))
    except Exception:
        topk = 20
    per_symbol_cfg = scoring_cfg.get("per_symbol", defaults.get("per_symbol"))
    if per_symbol_cfg is not None:
        try:
            per_symbol_cap = max(1, int(per_symbol_cfg))
        except Exception:
            per_symbol_cap = None
    else:
        per_symbol_cap = None
    budgets_cfg_raw = scoring_cfg.get("budgets") or defaults.get("budgets")
    if isinstance(budgets_cfg_raw, dict) and budgets_cfg_raw:
        budgets = {str(k): float(v) for k, v in budgets_cfg_raw.items()}
    else:
        budgets = {"LOW": 0.5, "MED": 0.3, "HIGH": 0.2}
    reserve_cfg = scoring_cfg.get("reserve_frac", defaults.get("reserve_frac"))
    try:
        reserve_frac = float(reserve_cfg)
    except Exception:
        reserve_frac = 0.20
    min_ppt_cfg = scoring_cfg.get("min_profit_per_trade", defaults.get("min_profit_per_trade", 0.0))
    try:
        min_profit_per_trade = float(min_ppt_cfg)
    except Exception:
        min_profit_per_trade = 0.0
    next_day_path = (
        scoring_cfg.get("next_day_path")
        or defaults.get("next_day_path")
        or "configs/pairs_next_day.yaml"
    )

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
    finally:
        await pool.close()

    series_raw = series_all
    series_all = {sym: list(series_raw.get(sym, [])) for sym in syms}

    if workers_override is not None:
        workers = workers_override
    else:
        env_workers = os.getenv("PAIRS_BT_WORKERS")
        if env_workers is not None:
            try:
                workers = int(env_workers)
            except ValueError:
                workers = 1
        else:
            workers = 1

    if workers <= 0:
        auto = max(1, mp.cpu_count() - 1)
        workers = min(auto, len(pairs)) or 1
    else:
        workers = min(workers, len(pairs)) or 1

    mode = "parallel" if workers > 1 else "sequential"
    print(f"[pairs-bt] running {len(pairs)} pairs with {workers} worker(s) ({mode})")

    summaries: List[Dict[str, Any]] = []
    ledger: List[Dict[str, Any]] = []
    ledger_by_pair: Dict[str, List[Dict[str, Any]]] = {}

    if workers > 1:
        ctx = _mp_context()
        jobs = list(enumerate(pairs, start=1))
        with ctx.Pool(
            processes=workers,
            initializer=_mp_init,
            initargs=(series_all, costs_cfg, s_utc, e_utc, flatten_cutoff_min, strategy_name),
        ) as pool_exec:
            results = pool_exec.map(_mp_worker, jobs)
        for idx, summ, rows in sorted(results, key=lambda r: r[0]):
            if summ:
                summaries.append(summ)
                ledger_by_pair[summ["pair"]] = rows or []
            if rows:
                ledger.extend(rows)
            if idx % 5 == 0 or idx == len(pairs):
                print(f"[pairs-bt] processed {idx}/{len(pairs)} pairs")
    else:
        for idx, pair_cfg in enumerate(pairs, 1):
            summ, rows = _simulate_pair_core(
                pair_cfg, series_all, costs_cfg, s_utc, e_utc, flatten_cutoff_min, strategy_name
            )
            if summ:
                summaries.append(summ)
                ledger_by_pair[summ["pair"]] = rows or []
            if rows:
                ledger.extend(rows)
            if idx % 5 == 0 or idx == len(pairs):
                print(f"[pairs-bt] processed {idx}/{len(pairs)} pairs")

    if not summaries:
        print("[pairs-bt] no summaries generated; exiting without writing output")
        return

    filtered = apply_constraints(summaries, gates)
    kept = len(filtered)
    dropped = len(summaries) - kept
    if dropped:
        print(f"[pairs-bt] constraints rejected {dropped} pair configs")
    scored = score_rows(filtered, weights)
    for idx, row in enumerate(scored, start=1):
        row["rank"] = idx
        row["bucket"] = _bucket_for_dd(row.get("max_dd"), dd_thresholds)
        row["win_rate_pct"] = _win_rate_percent(row)
        row["profit_per_trade"] = _profit_per_trade_value(row)
    ranked_rows = list(scored)

    top_preview = ranked_rows[:5]
    for r in top_preview:
        wr_pct = r.get("win_rate_pct", 0.0) or 0.0
        print(f"[pairs-bt] top#{r.get('rank', 0):02d} {r['pair']} tf={r['timeframe']} score={r.get('score', 0.0):.3f} pnl={r.get('pnl_total',0.0):.2f} trades={int(r.get('n_trades',0))} win_rate={wr_pct:.1f}%")

    latest_run = get_latest_run_id() or 0
    run_id = latest_run + 1
    results_for_save = []
    for summ in summaries:
        win_rate_pct = _win_rate_percent(summ)
        profit_pt = _profit_per_trade_value(summ)
        avg_hold = _avg_hold_minutes(summ)
        n_trades = int(summ.get("n_trades", summ.get("trades", 0)) or 0)
        pnl_total = float(summ.get("pnl_total") or 0.0)
        stats = {
            "pnl": pnl_total,
            "n_trades": n_trades,
            "win_rate": win_rate_pct,
            "profit_per_trade": profit_pt,
            "avg_hold_min": avg_hold,
        }
        params = _ensure_params_dict(summ.get("params"))
        bucket_val = summ.get("bucket") or _bucket_for_dd(summ.get("max_dd"), dd_thresholds)
        summ["bucket"] = bucket_val
        summ["win_rate_pct"] = win_rate_pct
        summ["profit_per_trade"] = profit_pt
        summ["avg_hold_min"] = avg_hold
        result = {
            "symbol": summ.get("symbol"),
            "strategy": summ.get("strategy"),
            "timeframe": int(summ.get("timeframe", summ.get("tf_min", 0)) or 0),
            "params": params,
            "stats": stats,
            "max_dd": float(summ.get("max_dd") or 0.0),
            "avg_hold_min": float(summ.get("avg_hold_min") or 0.0),
            "score": summ.get("score"),
            "rank": summ.get("rank"),
            "pair": summ.get("pair"),
            "bucket": bucket_val,
            "win_rate": win_rate_pct,
            "n_trades": n_trades,
            "risk_per_trade": risk_per_trade,
            "flatten_hhmm": summ.get("flatten_hhmm"),
        }
        trades_rows = ledger_by_pair.get(summ["pair"], [])
        result["trades"] = trades_rows
        results_for_save.append(result)

    save_results(run_id, results_for_save)
    print(f"[pairs-bt] results saved under run {run_id}")

    raw_picks: List[Dict[str, Any]] = []
    for row in ranked_rows:
        wr_pct = row.get("win_rate_pct", _win_rate_percent(row))
        if wr_pct < 60.0:
            continue
        profit_pt = row.get("profit_per_trade", _profit_per_trade_value(row))
        if profit_pt < min_profit_per_trade:
            continue
        bucket_val = row.get("bucket") or _bucket_for_dd(row.get("max_dd"), dd_thresholds)
        row["bucket"] = bucket_val
        stats_block = {
            "pnl": float(row.get("pnl_total") or 0.0),
            "n_trades": int(row.get("n_trades", 0) or 0),
            "win_rate": wr_pct,
            "profit_per_trade": profit_pt,
            "avg_hold_min": _avg_hold_minutes(row),
        }
        pick = {
            "symbol": str(row.get("symbol") or ""),
            "strategy": row.get("strategy"),
            "timeframe": int(row.get("timeframe", row.get("tf_min", 0)) or 0),
            "params": _ensure_params_dict(row.get("params")),
            "max_dd": float(row.get("max_dd") or 0.0),
            "score": float(row.get("score") or 0.0),
            "bucket": bucket_val,
            "risk_per_trade": risk_per_trade,
            "stats": stats_block,
            "leg_x": row.get("leg_x"),
            "leg_y": row.get("leg_y"),
            "avg_hold_min": _avg_hold_minutes(row),
        }
        raw_picks.append(pick)

    print(
        f"[pairs-bt] raw candidates with win_rate>=60% and profit/trade>={min_profit_per_trade:.2f} -> {len(raw_picks)}"
    )

    dedup_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for pick in raw_picks:
        leg_x = str(pick.get("leg_x") or "")
        leg_y = str(pick.get("leg_y") or "")
        key = tuple(sorted((leg_x, leg_y)))
        if key in dedup_map:
            if pick.get("score", float("-inf")) > dedup_map[key].get("score", float("-inf")):
                dedup_map[key] = pick
        else:
            dedup_map[key] = pick
    dedup_picks = sorted(dedup_map.values(), key=lambda p: p.get("score", float("-inf")), reverse=True)
    print(f"[pairs-bt] after pair de-dup (highest score kept) -> {len(dedup_picks)}")

    picks: List[Dict[str, Any]] = []
    per_symbol_counts: Dict[str, int] = {}
    for pick in dedup_picks:
        symbol = pick.get("symbol", "")
        if per_symbol_cap is not None and symbol:
            if per_symbol_counts.get(symbol, 0) >= per_symbol_cap:
                continue
            per_symbol_counts[symbol] = per_symbol_counts.get(symbol, 0) + 1
        picks.append(pick)
        if len(picks) >= topk:
            break

    print(f"[pairs-bt] prepared {len(picks)} next-day selections after symbol cap (topk={topk})")
    write_next_day_yaml(next_day_path, picks, budgets, reserve_frac)

    os.makedirs("data/backtests", exist_ok=True)
    out_summary = "data/backtests/pairs_bt.csv"
    out_trades  = "data/backtests/pairs_bt_trades.csv"
    out_scored  = "data/backtests/pairs_bt_scored.csv"

    def _prepare_csv_rows(rows: List[Dict[str, Any]]) -> Tuple[List[str], List[Dict[str, Any]]]:
        if not rows:
            return [], []
        keys = sorted({k for r in rows for k in r.keys()})
        prepared: List[Dict[str, Any]] = []
        for r in rows:
            row = {}
            for k in keys:
                v = r.get(k)
                if isinstance(v, (dict, list)):
                    row[k] = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
                elif v is None:
                    row[k] = ""
                else:
                    row[k] = v
            prepared.append(row)
        return keys, prepared

    summaries_sorted = sorted(
        summaries,
        key=lambda r: (r.get("score") is not None, r.get("score", float("-inf"))),
        reverse=True,
    )
    keys_summary, rows_summary = _prepare_csv_rows(summaries_sorted)
    with open(out_summary, "w", newline="") as f:
        if rows_summary:
            writer = csv.DictWriter(f, fieldnames=keys_summary)
            writer.writeheader()
            writer.writerows(rows_summary)
        else:
            f.write("")

    keys_scored, rows_scored = _prepare_csv_rows(ranked_rows)
    with open(out_scored, "w", newline="") as f:
        if rows_scored:
            writer = csv.DictWriter(f, fieldnames=keys_scored)
            writer.writeheader()
            writer.writerows(rows_scored)
        else:
            f.write("")

    with open(out_trades, "w", newline="") as f:
        if ledger:
            keys_trades, rows_trades = _prepare_csv_rows(ledger)
            writer = csv.DictWriter(f, fieldnames=keys_trades)
            writer.writeheader()
            writer.writerows(rows_trades)
        else:
            f.write("")

    print(f"[pairs-bt] wrote summary -> {out_summary}")
    print(f"[pairs-bt] wrote scored  -> {out_scored}")
    print(f"[pairs-bt] wrote trades  -> {out_trades}")

    conn = await asyncpg.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        has_tbl = await conn.fetchval("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema='public' AND table_name='backtest_pairs_results'
            );
        """)
        if has_tbl and summaries:
            await conn.executemany("""
              INSERT INTO backtest_pairs_results
              (pair, tf_min, n_bars, trades, win_rate, pnl_total, pnl_avg, run_ts)
              VALUES($1,$2,$3,$4,$5,$6,$7, now())
            """, [(s["pair"], s["tf_min"], s["n_bars"], s["trades"], s["win_rate"], s["pnl_total"], s["pnl_avg"]) for s in summaries])
    finally:
        await conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=None, help="Number of worker processes (<=0 for auto).")
    args = parser.parse_args()
    asyncio.run(main(workers_override=args.workers))
