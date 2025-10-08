# strategy/strategies/cci_reversion.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from collections import deque

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["CCIReversion"]


class CCIReversion(BaseStrategy):
    """
    Commodity Channel Index (CCI) mean-reversion.

    Behavior (state-based by default to match your earlier code):
      - tp = (H + L + C) / 3
      - CCI = (tp - SMA(tp, period)) / (0.015 * mean_abs_dev(tp, period))
      - If CCI > th_hi -> SELL (overbought)
      - If CCI < th_lo -> BUY  (oversold)
      - Else -> FLAT

    Usage (unchanged):
        strat = CCIReversion(period=20, th_hi=+100, th_lo=-100, edge_only=False)
        sig = strat.on_bar(candle)

    Params (self.params):
        period (int)        default 20
        th_hi (float)       default +100.0
        th_lo (float)       default -100.0
        edge_only (bool)    default False   # True = signal only when crossing thresholds
        stop_pct (float)    default 0.0
        tgt_pct  (float)    default 0.0

    Grid config support:
        timeframe: "1" | "3" | "5" | "15" | ...
        params_by_tf: { "<tf>": { period: int, th_hi?: float, th_lo?: float, edge_only?: bool } }
    """
    name = "CCI_REVERSION"

    _DEFAULTS = dict(
        period=20,
        th_hi=+100.0,
        th_lo=-100.0,
        edge_only=False,     # state-based by default (continuous while outside band)
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # rolling window of typical prices; and previous zone for edge detection
    _tp_win: deque[float]
    _prev_zone: Optional[int]   # -1 oversold, 0 inside, +1 overbought

    def __init__(
        self,
        *,
        # direct overrides
        period: Optional[int] = None,
        th_hi: Optional[float] = None,
        th_lo: Optional[float] = None,
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
            merged.update(tf_params)

        if period is not None:     merged["period"] = int(period)
        if th_hi is not None:      merged["th_hi"] = float(th_hi)
        if th_lo is not None:      merged["th_lo"] = float(th_lo)
        if edge_only is not None:  merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:   merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:    merged["tgt_pct"] = float(tgt_pct)

        # normalize
        merged["period"] = max(1, int(merged.get("period", 20)))
        merged["th_hi"] = float(merged.get("th_hi", +100.0))
        merged["th_lo"] = float(merged.get("th_lo", -100.0))

        super().__init__(params=merged, **base_kwargs)

        n = int(self.params["period"])
        self.warmup = n
        self._tp_win = deque(maxlen=n)
        self._prev_zone = None

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._tp_win.clear()
        self._prev_zone = None

    # -------- strategy core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        n = int(self.params["period"])
        th_hi = float(self.params["th_hi"])
        th_lo = float(self.params["th_lo"])
        edge_only = bool(self.params["edge_only"])

        # update typical price window
        tp = (bar.h + bar.l + bar.c) / 3.0
        self._tp_win.append(tp)

        if len(self._tp_win) < n:
            return Signal.flat("init")

        ma = _mean_deque(self._tp_win)
        md = _mad_deque(self._tp_win, ma)  # mean absolute deviation
        if md == 0.0:
            return Signal.flat("degenerate-md")

        cci = (tp - ma) / (0.015 * md)

        # determine zone relative to thresholds
        if cci > th_hi:
            zone = +1   # overbought
        elif cci < th_lo:
            zone = -1   # oversold
        else:
            zone = 0    # inside

        # first time with full window: set baseline
        if self._prev_zone is None:
            self._prev_zone = zone
            return Signal.flat("baseline")

        crossed_up = (self._prev_zone <= 0) and (zone == +1)   # inside/oversold -> overbought
        crossed_dn = (self._prev_zone >= 0) and (zone == -1)   # inside/overbought -> oversold
        self._prev_zone = zone

        # optional stops/targets
        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)
        px = bar.c
        stop_long = (px * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (px * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (px * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (px * (1 - tgt_pct))  if tgt_pct  > 0 else None

        reason = f"(n={n},th_hi={th_hi},th_lo={th_lo})"

        if edge_only:
            # mean-reversion: SELL when entering overbought; BUY when entering oversold
            if crossed_up:
                return Signal.sell(confidence=0.75, stop=stop_short, target=tgt_short,
                                   reason=f"cci_overbought {reason}")
            if crossed_dn:
                return Signal.buy(confidence=0.75, stop=stop_long, target=tgt_long,
                                  reason=f"cci_oversold {reason}")
            return Signal.flat("no-cross")

        # state-based (continuous stance while outside thresholds)
        if zone == +1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short,
                               reason=f"cci_overbought {reason}")
        elif zone == -1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long,
                              reason=f"cci_oversold {reason}")
        else:
            return Signal.flat("inside")

# ---- helpers on deque ----
def _mean_deque(xs: deque[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0

def _mad_deque(xs: deque[float], mu: float) -> float:
    n = len(xs)
    if n == 0:
        return 0.0
    return sum(abs(x - mu) for x in xs) / n
