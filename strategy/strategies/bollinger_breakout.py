# strategy/strategies/bollinger_breakout.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["BollingerBreakout"]


class BollingerBreakout(BaseStrategy):
    """
    Bollinger Band breakout strategy.

    Usage (unchanged):
        strat = BollingerBreakout(period=20, k=2.0, price="close", edge_only=True)
        strat.on_start("AAPL")          # optional
        sig = strat.on_bar(candle)      # same external call

    Params (self.params):
        period (int)        default 20      # rolling lookback
        k (float)           default 2.0     # stddev multiplier
        price (str)         default "close" # "close" or "typical" ((H+L+C)/3)
        edge_only (bool)    default True    # signal only on cross events
        stop_pct (float)    default 0.0
        tgt_pct  (float)    default 0.0

    Also supports grid configs:
        timeframe: "1" | "3" | "5" | "15" | ...
        params_by_tf: { "<tf>": { period|peiod: int, k: float, price?: "close"/"typical" } }
    """
    name = "BOLLINGER_BREAKOUT"

    _DEFAULTS = dict(
        period=20,
        k=2.0,
        price="close",      # "close" or "typical"
        edge_only=True,
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # previous position of price relative to bands:
    # -1 = below lower, 0 = inside, +1 = above upper, None = unknown
    _prev_zone: Optional[int]

    def __init__(
        self,
        *,
        # direct overrides
        period: Optional[int] = None,
        k: Optional[float] = None,
        price: Optional[str] = None,
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
        if price is not None:      merged["price"] = str(price).lower()
        if edge_only is not None:  merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:   merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:    merged["tgt_pct"] = float(tgt_pct)

        # normalize
        merged["period"] = max(1, int(merged.get("period", 20)))
        merged["k"] = float(merged.get("k", 2.0))
        psrc = str(merged.get("price", "close")).lower()
        merged["price"] = "typical" if psrc.startswith("typ") else "close"

        super().__init__(params=merged, **base_kwargs)

        self.warmup = int(self.params["period"])
        self._prev_zone = None

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._prev_zone = None

    # -------- strategy core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        n = int(self.params["period"])
        k = float(self.params["k"])
        edge_only = bool(self.params["edge_only"])
        price_src = self.params["price"]

        if len(hist) < n:
            return Signal.flat("init")

        # Select price series
        if price_src == "typical":
            vals = [ (b.h + b.l + b.c) / 3.0 for b in hist[-n:] ]
            p_now = (bar.h + bar.l + bar.c) / 3.0
        else:
            vals = [ b.c for b in hist[-n:] ]
            p_now = bar.c

        mean = _mean(vals)
        std = _std(vals, mean)
        upper = mean + k * std
        lower = mean - k * std

        # define current zone
        if p_now > upper:
            zone = +1
        elif p_now < lower:
            zone = -1
        else:
            zone = 0

        # first bar with bands: set baseline
        if self._prev_zone is None:
            self._prev_zone = zone
            return Signal.flat("baseline")

        crossed_up = self._prev_zone <= 0 and zone == +1  # from inside/below to above upper
        crossed_dn = self._prev_zone >= 0 and zone == -1  # from inside/above to below lower
        self._prev_zone = zone

        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)

        stop_long = (p_now * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (p_now * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (p_now * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (p_now * (1 - tgt_pct))  if tgt_pct  > 0 else None

        reason_suffix = f"(n={n},k={k},{price_src})"

        if edge_only:
            if crossed_up:
                return Signal.buy(confidence=0.8, stop=stop_long, target=tgt_long,
                                  reason=f"break↑upper {reason_suffix}")
            if crossed_dn:
                return Signal.sell(confidence=0.8, stop=stop_short, target=tgt_short,
                                   reason=f"break↓lower {reason_suffix}")
            return Signal.flat("no-cross")

        # Non-edge: maintain stance outside the bands
        if zone == +1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long,
                              reason=f"above upper {reason_suffix}")
        elif zone == -1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short,
                               reason=f"below lower {reason_suffix}")
        else:
            return Signal.flat("inside band")

# ---- small numerics helpers (simple, stable enough for trading bars) ----
def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0

def _std(xs: list[float], mu: float) -> float:
    # population std; switch to sample (ddof=1) if you prefer
    if not xs:
        return 0.0
    var = sum((x - mu) ** 2 for x in xs) / len(xs)
    return var ** 0.5
