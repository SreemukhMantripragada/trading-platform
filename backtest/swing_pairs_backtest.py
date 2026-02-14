"""
backtest/swing_pairs_backtest.py

Offline swing backtester that hydrates candles directly from Zerodha (Kite) for
the pairs YAML produced by analytics/pairs/swing_find_pairs.py.
"""
from __future__ import annotations

import argparse
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import yaml

from libs.zerodha_history import KiteHistoricalClient, interval_for_minutes

PAIR_BASE_NOTIONAL_DEFAULT = 200_000.0
PAIR_LEVERAGE_DEFAULT = 5.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Swing pairs backtest using Zerodha historical data.")
    parser.add_argument("--pairs", type=str, required=True, help="Path to swing pairs YAML.")
    parser.add_argument("--lookback-days", type=int, default=200, help="History depth to request from Kite.")
    parser.add_argument("--max-trade-minutes", type=int, default=0, help="Override max_hold_min in minutes.")
    parser.add_argument("--out", type=str, default="", help="Optional CSV output path.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def load_pairs_config(path: str) -> List[Dict[str, object]]:
    doc = yaml.safe_load(Path(path).read_text()) or {}
    return list(doc.get("selections") or [])


def fetch_candles(
    client: KiteHistoricalClient,
    symbol: str,
    tf_minutes: int,
    lookback_days: int,
) -> Tuple[np.ndarray, np.ndarray]:
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=lookback_days)
    interval = interval_for_minutes(tf_minutes)
    candles = client.fetch_interval(symbol, interval=interval, start=start, end=end)
    if not candles:
        return np.array([]), np.array([])
    ts = np.array([int(_ensure_dt(row["date"]).timestamp()) for row in candles], dtype=np.int64)
    close = np.array([float(row["close"]) for row in candles], dtype=np.float64)
    return ts, close


def _ensure_dt(val) -> datetime:
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    raise TypeError(f"Unexpected datetime type: {type(val)}")


def align_series(
    ts_a: np.ndarray, px_a: np.ndarray, ts_b: np.ndarray, px_b: np.ndarray
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    if len(ts_a) == 0 or len(ts_b) == 0:
        return np.array([]), np.array([]), np.array([])
    idx_a = {int(ts): i for i, ts in enumerate(ts_a)}
    idx_b = {int(ts): i for i, ts in enumerate(ts_b)}
    common = sorted(set(idx_a.keys()) & set(idx_b.keys()))
    if not common:
        return np.array([]), np.array([]), np.array([])
    arr_ts = np.array(common, dtype=np.int64)
    arr_a = np.array([px_a[idx_a[t]] for t in common], dtype=np.float64)
    arr_b = np.array([px_b[idx_b[t]] for t in common], dtype=np.float64)
    return arr_ts, arr_a, arr_b


def pair_gross_notional_from_params(params: Dict[str, float]) -> float:
    notional_leg = float(params.get("notional_per_leg", 0.0) or 0.0)
    base_notional = float(params.get("base_notional", 0.0) or 0.0)
    if base_notional <= 0 and notional_leg > 0:
        base_notional = notional_leg * 2.0
    if base_notional <= 0:
        base_notional = PAIR_BASE_NOTIONAL_DEFAULT
    leverage = float(params.get("leverage", PAIR_LEVERAGE_DEFAULT))
    if not math.isfinite(leverage) or leverage <= 0:
        leverage = PAIR_LEVERAGE_DEFAULT
    gross_override = float(params.get("gross_notional", 0.0) or 0.0)
    gross = gross_override if gross_override > 0 else base_notional * leverage
    return max(gross, base_notional)


def sized_quantities(px_a: float, px_b: float, beta: float, gross_notional: float) -> Tuple[int, int]:
    if gross_notional <= 0 or px_a <= 0 or px_b <= 0:
        return 0, 0
    beta_abs = abs(beta) if math.isfinite(beta) else 1.0
    beta_abs = max(beta_abs, 1e-3)
    ratio_a = 1.0
    ratio_b = beta_abs
    unit_cost = px_a * ratio_a + px_b * ratio_b
    if unit_cost <= 0:
        return 0, 0
    scale = gross_notional / unit_cost
    qty_a = max(1, int(scale * ratio_a))
    qty_b = max(1, int(scale * ratio_b))
    return qty_a, qty_b


@dataclass
class Trade:
    direction: int  # +1 = long spread, -1 = short spread
    entry_ts: int
    exit_ts: int
    entry_px_a: float
    entry_px_b: float
    exit_px_a: float
    exit_px_b: float
    pnl: float
    hold_min: float
    qty_a: int
    qty_b: int


class RollingWindow:
    def __init__(self, lookback: int):
        self.lookback = lookback
        self.x = deque(maxlen=lookback)
        self.y = deque(maxlen=lookback)

    def push(self, xv: float, yv: float) -> None:
        self.x.append(xv)
        self.y.append(yv)

    def ready(self) -> bool:
        return len(self.x) >= self.lookback

    def beta(self) -> Optional[float]:
        if len(self.x) < 5:
            return None
        x = np.array(self.x, dtype=np.float64)
        y = np.array(self.y, dtype=np.float64)
        x1 = np.vstack([np.ones_like(x), x]).T
        try:
            beta = np.linalg.lstsq(x1, y, rcond=None)[0][1]
        except np.linalg.LinAlgError:
            return None
        return float(beta)

    def zscore(self) -> Optional[float]:
        if len(self.x) < 5:
            return None
        spread = np.array(self.y) - np.array(self.x)
        mean = spread.mean()
        std = spread.std()
        if std <= 1e-9:
            return None
        return float((spread[-1] - mean) / std)


def simulate_pair(
    ts: np.ndarray,
    px_a: np.ndarray,
    px_b: np.ndarray,
    tf_minutes: int,
    params: Dict[str, float],
    *,
    max_trade_override: int = 0,
    verbose: bool = False,
) -> Tuple[Dict[str, float], List[Trade]]:
    lookback = int(params.get("lookback", 160))
    beta_lookback = int(params.get("beta_lookback", lookback))
    entry_z = float(params.get("entry_z", 2.0))
    exit_z = float(params.get("exit_z", 1.0))
    stop_z = float(params.get("stop_z", 3.0))
    gross_notional = pair_gross_notional_from_params(params)
    entry_fresh = int(params.get("entry_fresh_bars", 1))
    max_hold_min = int(params.get("max_hold_min", 72 * 60))
    if max_trade_override > 0:
        max_hold_min = max_trade_override

    log_a = deque(maxlen=max(beta_lookback, lookback))
    log_b = deque(maxlen=max(beta_lookback, lookback))
    spreads = deque(maxlen=lookback)

    position = 0
    entry_ts = None
    entry_px_a = entry_px_b = 0.0
    trades: List[Trade] = []
    last_z = None
    pending = None  # (direction, bars_since)
    max_hold_sec = max_hold_min * 60
    entry_qty_a = 0
    entry_qty_b = 0

    for idx, (t, pa, pb) in enumerate(zip(ts, px_a, px_b)):
        if pa <= 0 or pb <= 0:
            continue
        la = math.log(pa)
        lb = math.log(pb)
        log_a.append(la)
        log_b.append(lb)
        if len(log_a) < 5 or len(log_b) < 5:
            continue

        if len(log_a) >= beta_lookback:
            beta = rolling_beta(list(log_a)[-beta_lookback:], list(log_b)[-beta_lookback:])
        else:
            beta = rolling_beta(list(log_a), list(log_b))

        spread_val = la - beta * lb
        spreads.append(spread_val)
        z = compute_zscore(spreads)
        if z is not None and position == 0:
            if last_z is not None:
                crossed_upper = last_z < entry_z <= z
                crossed_lower = last_z > -entry_z >= z
                if crossed_upper:
                    pending = (-1, 0)
                elif crossed_lower:
                    pending = (1, 0)
            if pending and pending[1] > entry_fresh:
                pending = None
            if pending:
                direction, bars_since = pending
                if direction == -1 and z >= entry_z:
                    qty_a, qty_b = sized_quantities(pa, pb, beta, gross_notional)
                    if qty_a <= 0 or qty_b <= 0:
                        pending = None
                        continue
                    position = -1
                    entry_ts = t
                    entry_px_a = pa
                    entry_px_b = pb
                    entry_qty_a = qty_a
                    entry_qty_b = qty_b
                    pending = None
                    if verbose:
                        print(f"[enter] short spread {entry_ts} {entry_px_a} {entry_px_b} z={z:.2f}")
                elif direction == 1 and z <= -entry_z:
                    qty_a, qty_b = sized_quantities(pa, pb, beta, gross_notional)
                    if qty_a <= 0 or qty_b <= 0:
                        pending = None
                        continue
                    position = 1
                    entry_ts = t
                    entry_px_a = pa
                    entry_px_b = pb
                    entry_qty_a = qty_a
                    entry_qty_b = qty_b
                    pending = None
                    if verbose:
                        print(f"[enter] long spread {entry_ts} {entry_px_a} {entry_px_b} z={z:.2f}")
                else:
                    pending = (direction, bars_since + 1)
        elif z is not None and position != 0:
            exit_reason = None
            if abs(z) <= exit_z:
                exit_reason = "revert"
            elif abs(z) >= stop_z:
                exit_reason = "stop"
            elif entry_ts is not None and (t - entry_ts) >= max_hold_sec:
                exit_reason = "timeout"
            if exit_reason:
                pnl = compute_pnl(position, entry_px_a, entry_px_b, pa, pb, entry_qty_a, entry_qty_b)
                hold_min = ((t - entry_ts) / 60.0) if entry_ts else 0.0
                trades.append(
                    Trade(
                        direction=position,
                        entry_ts=entry_ts or t,
                        exit_ts=t,
                        entry_px_a=entry_px_a,
                        entry_px_b=entry_px_b,
                        exit_px_a=pa,
                        exit_px_b=pb,
                        pnl=pnl,
                        hold_min=hold_min,
                        qty_a=entry_qty_a,
                        qty_b=entry_qty_b,
                    )
                )
                if verbose:
                    print(f"[exit] {exit_reason} pnl={pnl:.2f} hold={hold_min:.1f}m")
                position = 0
                entry_ts = None
                entry_qty_a = 0
                entry_qty_b = 0
                pending = None
        last_z = z

    pnl_total = sum(tr.pnl for tr in trades)
    wins = sum(1 for tr in trades if tr.pnl > 0)
    avg_hold = sum(tr.hold_min for tr in trades) / len(trades) if trades else 0.0
    summary = {
        "pnl_total": pnl_total,
        "n_trades": len(trades),
        "win_rate": (wins / len(trades) * 100.0) if trades else 0.0,
        "avg_hold_min": avg_hold,
    }
    return summary, trades


def rolling_beta(x: Iterable[float], y: Iterable[float]) -> float:
    arr_x = np.array(list(x), dtype=np.float64)
    arr_y = np.array(list(y), dtype=np.float64)
    arr_x1 = np.vstack([np.ones_like(arr_x), arr_x]).T
    beta = np.linalg.lstsq(arr_x1, arr_y, rcond=None)[0][1]
    return float(beta)


def compute_zscore(values: deque) -> Optional[float]:
    if len(values) < 5:
        return None
    arr = np.array(values, dtype=np.float64)
    mean = arr.mean()
    std = arr.std()
    if std <= 1e-9:
        return None
    return float((arr[-1] - mean) / std)


def compute_pnl(
    direction: int,
    entry_a: float,
    entry_b: float,
    exit_a: float,
    exit_b: float,
    qty_a: float,
    qty_b: float,
) -> float:
    qty_a = max(1.0, float(qty_a))
    qty_b = max(1.0, float(qty_b))
    if direction == -1:  # short spread -> short A, long B
        pnl_a = (entry_a - exit_a) * qty_a
        pnl_b = (exit_b - entry_b) * qty_b
    else:  # long spread
        pnl_a = (exit_a - entry_a) * qty_a
        pnl_b = (entry_b - exit_b) * qty_b
    return float(pnl_a + pnl_b)


def main() -> None:
    args = parse_args()
    selections = load_pairs_config(args.pairs)
    if not selections:
        raise SystemExit("Pairs config empty.")
    client = KiteHistoricalClient()
    summaries: List[Dict[str, object]] = []
    for row in selections:
        symbol = str(row.get("symbol") or "")
        if "-" not in symbol:
            continue
        leg_a, leg_b = [s.strip().upper() for s in symbol.split("-", 1)]
        tf = int(row.get("timeframe") or 0)
        params = dict((row.get("params") or {}))
        ts_a, px_a = fetch_candles(client, leg_a, tf, args.lookback_days)
        ts_b, px_b = fetch_candles(client, leg_b, tf, args.lookback_days)
        ts, arr_a, arr_b = align_series(ts_a, px_a, ts_b, px_b)
        if len(ts) == 0:
            continue
        summary, trades = simulate_pair(
            ts,
            arr_a,
            arr_b,
            tf,
            params,
            max_trade_override=args.max_trade_minutes,
            verbose=args.verbose,
        )
        summaries.append(
            {
                "symbol": symbol,
                "timeframe": tf,
                **summary,
            }
        )
        print(
            f"[bt] {symbol} tf={tf}m trades={summary['n_trades']} pnl={summary['pnl_total']:.2f} "
            f"win={summary['win_rate']:.1f}% avg_hold={summary['avg_hold_min']:.1f}m"
        )

    if args.out and summaries:
        import csv

        with open(args.out, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(summaries[0].keys()))
            writer.writeheader()
            for row in summaries:
                writer.writerow(row)
        print(f"[bt] wrote summary csv → {args.out}")
    elif args.out:
        print("[bt] no trades to export; skipping CSV write.")


if __name__ == "__main__":
    main()
