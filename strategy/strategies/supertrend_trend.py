# strategy/strategies/supertrend_trend.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from collections import deque

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["SupertrendTrend"]


class SupertrendTrend(BaseStrategy):
    """
    Supertrend (trend-following).

    Usage (unchanged):
        strat = SupertrendTrend(atr_period=10, mult=3.0, edge_only=True)
        sig = strat.on_bar(candle)

    Params (self.params):
        atr_period (int)    default 10    # ATR lookback (SMA of TR)
        mult (float)        default 3.0   # band multiplier
        edge_only (bool)    default True  # only signal on flips; False -> continuous stance
        stop_pct (float)    default 0.0
        tgt_pct  (float)    default 0.0

    Grid config:
        timeframe: "1" | "3" | "5" | "15" | ...
        params_by_tf: { "<tf>": { atr_period|atr: int, mult|k: float, edge_only?: bool } }
    """
    name = "SUPERTREND"

    _DEFAULTS = dict(
        atr_period=10,
        mult=3.0,
        edge_only=True,
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # --- state ---
    _prev_close: Optional[float]
    _tr_window: deque[float]       # rolling TRs for ATR (SMA)
    _upper: Optional[float]        # final upper band (trailing)
    _lower: Optional[float]        # final lower band (trailing)
    _trend: Optional[int]          # +1 uptrend, -1 downtrend
    _prev_trend: Optional[int]

    def __init__(
        self,
        *,
        atr_period: Optional[int] = None,
        mult: Optional[float] = None,
        edge_only: Optional[bool] = None,
        stop_pct: Optional[float] = None,
        tgt_pct: Optional[float] = None,
        # grid style
        timeframe: Optional[str] = None,
        params_by_tf: Optional[Dict[str, Dict[str, Any]]] = None,
        # passthrough dict
        params: Optional[dict] = None,
        **base_kwargs,
    ) -> None:
        merged = dict(self._DEFAULTS)
        if params:
            merged.update(params)

        if params_by_tf and timeframe is not None and timeframe in params_by_tf:
            tf_params = dict(params_by_tf[timeframe])
            if "atr_period" not in tf_params and "atr" in tf_params:
                tf_params["atr_period"] = tf_params["atr"]
            if "mult" not in tf_params and "k" in tf_params:
                tf_params["mult"] = tf_params["k"]
            merged.update(tf_params)

        if atr_period is not None: merged["atr_period"] = int(atr_period)
        if mult is not None:       merged["mult"] = float(mult)
        if edge_only is not None:  merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:   merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:    merged["tgt_pct"] = float(tgt_pct)

        merged["atr_period"] = max(1, int(merged.get("atr_period", 10)))
        merged["mult"] = float(merged.get("mult", 3.0))

        super().__init__(params=merged, **base_kwargs)

        ap = int(self.params["atr_period"])
        self.warmup = ap + 1  # need at least ap TRs = ap+1 closes

        self._prev_close = None
        self._tr_window = deque(maxlen=ap)
        self._upper = None
        self._lower = None
        self._trend = None
        self._prev_trend = None

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._prev_close = None
        self._tr_window.clear()
        self._upper = None
        self._lower = None
        self._trend = None
        self._prev_trend = None

    # -------- core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        ap = int(self.params["atr_period"])
        mult = float(self.params["mult"])
        edge_only = bool(self.params["edge_only"])

        h, l, c = bar.h, bar.l, bar.c

        # ---- ATR update (SMA of TR) ----
        if self._prev_close is None:
            self._prev_close = c
            return Signal.flat("init-prev-close")
        tr = max(h - l, abs(h - self._prev_close), abs(l - self._prev_close))
        self._tr_window.append(tr)
        self._prev_close = c

        if len(self._tr_window) < ap:
            return Signal.flat("init-atr")

        atr = sum(self._tr_window) / ap

        # ---- Basic bands ----
        mid = 0.5 * (h + l)                  # HL2
        basic_upper = mid + mult * atr
        basic_lower = mid - mult * atr

        # ---- Final (trailing) bands (use previous bands and previous close) ----
        # If previous final upper exists and close_prev <= prev_upper, keep the tighter (min) of basic and prev
        # Else reset to basic. (Symmetric rule for final lower)
        prev_upper = self._upper
        prev_lower = self._lower
        close_prev = hist[-2].c if len(hist) >= 2 else c  # safe guard

        if prev_upper is None:
            final_upper = basic_upper
        else:
            final_upper = basic_upper if close_prev > prev_upper else min(basic_upper, prev_upper)

        if prev_lower is None:
            final_lower = basic_lower
        else:
            final_lower = basic_lower if close_prev < prev_lower else max(basic_lower, prev_lower)

        # ---- Trend detection ----
        prev_trend = self._trend
        if prev_trend is None:
            # initialize trend
            if c > final_upper:
                trend = +1
            elif c < final_lower:
                trend = -1
            else:
                # default to previous trend if any, else neutral
                trend = +1 if (close_prev > (prev_upper or final_upper)) else -1 if (close_prev < (prev_lower or final_lower)) else 0
        else:
            if c > final_upper:
                trend = +1
            elif c < final_lower:
                trend = -1
            else:
                trend = prev_trend

        # persist bands and trend
        self._upper = final_upper
        self._lower = final_lower
        self._prev_trend = self._trend
        self._trend = trend

        # ---- Signals ----
        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)
        stop_long = (c * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (c * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (c * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (c * (1 - tgt_pct))  if tgt_pct  > 0 else None

        flipped_up = (self._prev_trend is not None) and (self._prev_trend <= 0) and (trend == +1)
        flipped_dn = (self._prev_trend is not None) and (self._prev_trend >= 0) and (trend == -1)

        reason = f"(atr={ap},mult={mult})"

        if edge_only:
            if flipped_up:
                return Signal.buy(confidence=0.8, stop=stop_long, target=tgt_long,
                                  reason=f"supertrend_up {reason}")
            if flipped_dn:
                return Signal.sell(confidence=0.8, stop=stop_short, target=tgt_short,
                                   reason=f"supertrend_down {reason}")
            return Signal.flat("no-flip")

        # Non-edge (state-based): hold stance while the trend persists
        if trend == +1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long,
                              reason=f"trend_up {reason}")
        elif trend == -1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short,
                               reason=f"trend_down {reason}")
        else:
            return Signal.flat("neutral")
