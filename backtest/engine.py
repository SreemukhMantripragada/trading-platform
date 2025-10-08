# backtest/engine.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple, Iterable
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from math import floor
from strategy.base import Candle, Signal

IST = ZoneInfo("Asia/Kolkata")
SQUAREOFF_T = time(15, 25)  # 15:25 IST

# ---------------- bar normalization ----------------
def _to_rows(bars) -> List[Tuple[datetime, float, float, float, float, int]]:
    """
    Normalize bars to list of tuples (ts, o, h, l, c, vol).
    Accepts: pandas.DataFrame with columns ts,o,h,l,c,vol OR list/dicts.
    """
    out = []
    # pandas DataFrame path
    try:
        import pandas as pd  # noqa
        if hasattr(bars, "itertuples"):
            for r in bars.itertuples(index=False):
                ts = r.ts.to_pydatetime() if hasattr(r.ts, "to_pydatetime") else r.ts
                out.append((ts, float(r.o), float(r.h), float(r.l), float(r.c), int(r.vol)))
            return out
    except Exception:
        pass
    # generic iterables
    for r in bars:
        if isinstance(r, dict):
            ts = r["ts"]
            if hasattr(ts, "to_pydatetime"):
                ts = ts.to_pydatetime()
            out.append((ts, float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"]), int(r["vol"])))
        else:
            # assume tuple-like
            ts = r[0]
            if hasattr(ts, "to_pydatetime"):
                ts = ts.to_pydatetime()
            out.append((ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), int(r[5])))
    return out

def _is_squareoff(ts_utc: datetime) -> bool:
    lt = ts_utc.astimezone(IST).time()
    return lt >= SQUAREOFF_T

def _mk_candle(symbol: str, ts: datetime, o: float, h: float, l: float, c: float, vol: int) -> Candle:
    return Candle(symbol=symbol, ts=ts, o=o, h=h, l=l, c=c, vol=vol)

# ---------------- sizing & costs ----------------
def _qty_from_capital(per_stock_capital: float, price: float) -> int:
    if price <= 0:
        return 0
    return max(0, int(floor(per_stock_capital / price)))

def _per_side_cost(price: float, qty: int, costs: Dict[str, Any]) -> float:
    notional = abs(float(price) * int(qty))
    slippage_bps = float(costs.get("slippage_bps", 0.0))
    pct_taxes    = float(costs.get("pct_taxes", 0.0))
    flat         = float(costs.get("flat_per_side", 0.0))
    return notional * (slippage_bps / 10_000.0) + notional * pct_taxes + flat

def _net_long(entry: float, exit_: float, qty: int, costs: Dict[str, Any]) -> float:
    gross = (float(exit_) - float(entry)) * int(qty)
    return gross - (_per_side_cost(entry, qty, costs) + _per_side_cost(exit_, qty, costs))

def _net_short(entry: float, exit_: float, qty_abs: int, costs: Dict[str, Any]) -> float:
    gross = (float(entry) - float(exit_)) * int(qty_abs)
    return gross - (_per_side_cost(entry, qty_abs, costs) + _per_side_cost(exit_, qty_abs, costs))

# ---------------- main ----------------
def run_backtest(symbol: str,
                 strategy_cls,
                 tf: int,
                 params: Dict[str, Any],
                 bars,
                 *,
                 per_stock_capital: float = 100_000.0,
                 exit_policy: Dict[str, Any] | None = None,
                 allow_short: bool = True,
                 costs: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Run a single backtest for (symbol, strategy, timeframe, params).

    Additions vs original:
      - Position size = floor(per_stock_capital / entry_price)
      - Costs on entry & exit: slippage_bps, pct_taxes, flat_per_side
      - Square-off remains at 15:25 IST
      - Exit policy:
          mode='strategy' (default): strategy controls exits/opposites.
          mode='fixed': use tp_pct/sl_pct (percent) from entry; strategy only opens.
        exit_policy example:
          {mode: "fixed", tp_pct: 1.0, sl_pct: 0.8, squareoff_ist: "15:25"}
    """
    rows = _to_rows(bars)
    if not rows:
        return {
            "symbol": symbol,
            "strategy": strategy_cls.__name__,
            "timeframe": tf,
            "params": params or {},
            "max_dd": 0.0,
            "stats": {"pnl": 0.0, "n_trades": 0, "win_rate": 0.0, "profit_per_trade": 0.0},
            "trades": [],
        }

    EP = exit_policy or {}
    mode = str(EP.get("mode", "strategy")).lower()
    # tp/sl as fractions
    tp_frac = float(EP.get("tp_pct", 0.0)) / 100.0
    sl_frac = float(EP.get("sl_pct", 0.0)) / 100.0
    # custom cutoff if provided (kept default 15:25 IST)
    cutoff_raw = EP.get("squareoff_ist")
    global SQUAREOFF_T
    if isinstance(cutoff_raw, str) and ":" in cutoff_raw:
        hh, mm = cutoff_raw.split(":")
        SQUAREOFF_T = time(int(hh), int(mm))

    C = costs or {}

    # --- instantiate strategy and call on_start if present ---
    strat = strategy_cls(**(params or {}))
    if hasattr(strat, "on_start"):
        try:
            strat.on_start(symbol)
        except Exception:
            pass

    pos = 0            # negative short, positive long, 0 flat
    qty = 0
    entry_px: Optional[float] = None
    entry_ts: Optional[datetime] = None
    open_reason: Optional[str] = None
    fixed_sl: Optional[float] = None
    fixed_tp: Optional[float] = None

    trades: List[Dict[str, Any]] = []
    pnl_total = 0.0

    # equity curve for DD (in absolute currency after costs)
    eq = 0.0
    peak = 0.0
    max_dd_mag = 0.0  # report magnitude

    def _record_exit(exit_ts: datetime, exit_px: float, reason: str):
        nonlocal pos, qty, entry_px, entry_ts, pnl_total, eq, peak, max_dd_mag, fixed_sl, fixed_tp
        if pos == 0 or entry_px is None or qty <= 0:
            return
        if pos > 0:
            pnl = _net_long(entry_px, exit_px, qty, C)
            side = "LONG"
        else:
            pnl = _net_short(entry_px, exit_px, qty, C)
            side = "SHORT"

        pnl_total += pnl
        eq += pnl
        peak = max(peak, eq)
        dd = peak - eq  # amount below peak
        if dd > max_dd_mag:
            max_dd_mag = dd

        trades.append({
            "ts_entry": entry_ts,
            "px_entry": float(entry_px),
            "side": side,
            "qty": int(qty),
            "ts_exit": exit_ts,
            "px_exit": float(exit_px),
            "pnl": float(pnl),
            "reason": reason,
            "open_reason": open_reason,
        })
        pos = 0
        qty = 0
        entry_px = None
        entry_ts = None
        fixed_sl = None
        fixed_tp = None

    # main loop
    for (ts, o, h, l, c, vol) in rows:
        bar = _mk_candle(symbol, ts, o, h, l, c, vol)

        # Pre-cutoff square-off if open
        if pos != 0 and _is_squareoff(ts):
            _record_exit(ts, c, "squareoff")
            # continue evaluating signal after squareoff (flat now)

        # Get strategy signal (Candle-first, fallback to live signature)
        try:
            sig: Signal = strat.on_bar(bar)  # type: ignore
            action = getattr(sig, "action", getattr(sig, "side", "HOLD")) or "HOLD"
            stop = getattr(sig, "stop", getattr(sig, "stop_loss", None))
            target = getattr(sig, "target", getattr(sig, "take_profit", None))
            reason = getattr(sig, "reason", None)
        except TypeError:
            tf_str = f"{tf}m"
            sig = strat.on_bar(symbol, tf_str, ts, o, h, l, c, vol, {})  # type: ignore
            action = getattr(sig, "action", getattr(sig, "side", "HOLD")) or "HOLD"
            stop = getattr(sig, "stop", getattr(sig, "stop_loss", None))
            target = getattr(sig, "target", getattr(sig, "take_profit", None))
            reason = getattr(sig, "reason", None)

        # ----- entries -----
        if pos == 0:
            if action == "BUY":
                q = _qty_from_capital(per_stock_capital, c)
                if q > 0:
                    pos = +1
                    qty = q
                    entry_px = c
                    entry_ts = ts
                    open_reason = reason or "buy"
                    if mode == "fixed" and (tp_frac > 0 or sl_frac > 0):
                        fixed_tp = entry_px * (1.0 + tp_frac)
                        fixed_sl = entry_px * (1.0 - sl_frac)
                    else:
                        fixed_tp = (float(target) if target is not None else None)
                        fixed_sl = (float(stop) if stop is not None else None)

            elif action == "SELL" and allow_short:
                q = _qty_from_capital(per_stock_capital, c)
                if q > 0:
                    pos = -1
                    qty = q
                    entry_px = c
                    entry_ts = ts
                    open_reason = reason or "sell"
                    if mode == "fixed" and (tp_frac > 0 or sl_frac > 0):
                        fixed_tp = entry_px * (1.0 - tp_frac)
                        fixed_sl = entry_px * (1.0 + sl_frac)
                    else:
                        fixed_tp = (float(target) if target is not None else None)
                        fixed_sl = (float(stop) if stop is not None else None)

        else:
            # ----- exits -----
            exit_now = False
            exit_px: Optional[float] = None
            exit_reason = None

            if mode == "fixed":
                if pos > 0:
                    hit_sl = (fixed_sl is not None) and (l <= fixed_sl)
                    hit_tp = (fixed_tp is not None) and (h >= fixed_tp)
                    if hit_sl or hit_tp:
                        exit_px = float(fixed_sl if hit_sl else fixed_tp)
                        # constrain to bar
                        exit_px = min(max(exit_px, l), h)
                        exit_reason = "stop" if hit_sl else "target"
                        exit_now = True
                else:
                    hit_sl = (fixed_sl is not None) and (h >= fixed_sl)
                    hit_tp = (fixed_tp is not None) and (l <= fixed_tp)
                    if hit_sl or hit_tp:
                        exit_px = float(fixed_sl if hit_sl else fixed_tp)
                        exit_px = min(max(exit_px, l), h)
                        exit_reason = "stop" if hit_sl else "target"
                        exit_now = True
            else:
                # strategy-driven exits/opposites
                if (pos > 0 and action in ("SELL", "EXIT")) or (pos < 0 and action in ("BUY", "EXIT")):
                    exit_px = c
                    exit_reason = reason or action
                    exit_now = True

            if exit_now and exit_px is not None:
                _record_exit(ts, exit_px, exit_reason or "exit")

        # End-of-bar square-off (already checked at top, kept for safety if cutoff was hit mid-loop)
        if pos != 0 and _is_squareoff(ts):
            _record_exit(ts, c, "squareoff")

    # end of series: close if still open
    if pos != 0 and entry_px is not None:
        last_ts, _, _, _, last_c, _ = rows[-1]
        _record_exit(last_ts, last_c, "end")

    # stats
    n_trades = len(trades)
    wins = sum(1 for t in trades if t["pnl"] > 0)
    win_rate = (wins / n_trades * 100.0) if n_trades > 0 else 0.0
    profit_per_trade = (pnl_total / n_trades) if n_trades > 0 else 0.0

    # on_end hook
    if hasattr(strat, "on_end"):
        try:
            strat.on_end(symbol)
        except Exception:
            pass

    return {
        "symbol": symbol,
        "strategy": strategy_cls.__name__,
        "timeframe": tf,
        "params": params or {},
        "max_dd": float(max_dd_mag),
        "stats": {
            "pnl": float(pnl_total),
            "n_trades": int(n_trades),
            "win_rate": float(win_rate),
            "profit_per_trade": float(profit_per_trade),
        },
        "trades": trades,  # keep for audit
    }
