# strategy/strategies/bollinger_reversion.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from collections import deque

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["BollingerReversion"]


class BollingerReversion(BaseStrategy):
    """
    Bollinger Band mean-reversion strategy.

    Behavior:
      - If price is above the upper band -> SELL (expect revert down)
      - If price is below the lower band -> BUY  (expect revert up)
      - Else -> FLAT

    Usage (unchanged):
        strat = BollingerReversion(period=20, k=2.0, edge_only=False)
        sig = strat.on_bar(candle)

    Params (self.params):
        period (int)        default 20      # lookback for rolling mean/std
        k (float)           default 2.0     # stddev multiplier
        edge_only (bool)    default False   # if True, emit only when leaving the band (inside->outside)
        stop_pct (float)    default 0.0
        tgt_pct  (float)    default 0.0

    Grid config:
        timeframe: "1" | "3" | "5" | "15" | ...
        params_by_tf: { "<tf>": { period|peiod: int, k: float, edge_only?: bool } }
    """
    name = "BOLLINGER_REVERSION"

    _DEFAULTS = dict(
        period=20,
        k=2.0,
        edge_only=False,     # state-based by default (matches your earlier version)
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # internal rolling window and previous zone
    # _zone: -1 = below lower, 0 = inside, +1 = above upper, None = unknown
    _win: deque[float]
    _prev_zone: Optional[int]

    def __init__(
        self,
        *,
        # direct overrides
        period: Optional[int] = None,
        k: Optional[float] = None,
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
            if "period" not in tf_params and "peiod" in tf_params:
                tf_params["period"] = tf_params["peiod"]
            merged.update(tf_params)

        if period is not None:     merged["period"] = int(period)
        if k is not None:          merged["k"] = float(k)
        if edge_only is not None:  merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:   merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:    merged["tgt_pct"] = float(tgt_pct)

        # normalize
        merged["period"] = max(1, int(merged.get("period", 20)))
        merged["k"] = float(merged.get("k", 2.0))

        super().__init__(params=merged, **base_kwargs)

        n = int(self.params["period"])
        self.warmup = n
        self._win = deque(maxlen=n)
        self._prev_zone = None

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._win.clear()
        self._prev_zone = None

    # -------- strategy core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        n = int(self.params["period"])
        k = float(self.params["k"])
        edge_only = bool(self.params["edge_only"])

        # rolling window of closes (efficient; avoids re-walking entire hist every time)
        self._win.append(bar.c)
        if len(self._win) < n:
            return Signal.flat("init")

        # rolling stats (population std)
        mean = _mean_deque(self._win)
        std = _std_deque(self._win, mean)
        upper = mean + k * std
        lower = mean - k * std

        p_now = bar.c
        if p_now > upper:
            zone = +1
        elif p_now < lower:
            zone = -1
        else:
            zone = 0

        # baseline on first computed bands
        if self._prev_zone is None:
            self._prev_zone = zone
            return Signal.flat("baseline")

        crossed_up = (self._prev_zone <= 0) and (zone == +1)  # inside/below -> above upper
        crossed_dn = (self._prev_zone >= 0) and (zone == -1)  # inside/above -> below lower
        self._prev_zone = zone

        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)

        stop_long = (p_now * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (p_now * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (p_now * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (p_now * (1 - tgt_pct))  if tgt_pct  > 0 else None

        reason = f"(n={n},k={k})"

        if edge_only:
            # Emit one-shot when leaving the band (inside -> outside)
            if crossed_up:
                return Signal.sell(confidence=0.8, stop=stop_short, target=tgt_short,
                                   reason=f"revert@upper {reason}")
            if crossed_dn:
                return Signal.buy(confidence=0.8, stop=stop_long, target=tgt_long,
                                  reason=f"revert@lower {reason}")
            return Signal.flat("no-cross")

        # State-based (matches your earlier implementation):
        if zone == +1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short,
                               reason=f"above upper {reason}")
        elif zone == -1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long,
                              reason=f"below lower {reason}")
        else:
            return Signal.flat("inside band")


# ---- tiny numerics helpers on deques ----
def _mean_deque(xs: deque[float]) -> float:
    # len(xs) > 0 by construction in calls
    return sum(xs) / len(xs)

def _std_deque(xs: deque[float], mu: float) -> float:
    # population std; switch to sample (ddof=1) if you prefer (guard len>1)
    n = len(xs)
    if n == 0:
        return 0.0
    var = sum((x - mu) * (x - mu) for x in xs) / n
    return var ** 0.5
