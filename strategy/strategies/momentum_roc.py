# strategy/strategies/momentum_roc.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any, Deque
from collections import deque

from strategy.base import BaseStrategy, Candle, Signal

class ROCMomentum(BaseStrategy):
    """
    Momentum strategy using Rate of Change (ROC).

    Features:
      - Zero-line mode (default): compare ROC against 0 (with optional deadband `band`)
      - Signal-line mode: compare ROC against EMA(ROC) of length `signal`
      - Edge-triggered vs state-based signalling (edge_only)
      - Per-timeframe config via `params_by_tf` with support for typo `peiod`

    Typical usage:
        strat = ROCMomentum(timeframe="3",
                            params_by_tf={"3": {"peiod": 20, "band": 0.25}},
                            compare_to_signal=False, edge_only=True)
        sig = strat.on_bar(candle)   # same external call

    Parameters (self.params):
        period (int)                default 12      # ROC lookback
        band (float)                default 0.0     # deadband half-width around 0 or signal
        pct (bool)                  default True    # scale ROC by 100
        compare_to_signal (bool)    default False   # if True, compare to EMA(ROC)
        signal (int)                default 6       # EMA period for ROC when compare_to_signal=True
        edge_only (bool)            default True    # emit only on crossings (edge)
        stop_pct (float)            default 0.0     # optional stop % (e.g., 0.006)
        tgt_pct (float)             default 0.0     # optional target % (e.g., 0.015)
    """
    name = "ROC_MOMENTUM"

    _DEFAULTS = dict(
        period=12,
        band=0.0,
        pct=True,
        compare_to_signal=False,
        signal=6,
        edge_only=True,
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # internal state
    _prev_diff: Optional[float]
    _roc_signal: Optional[float]
    _roc_seed: Deque[float]

    def __init__(
        self,
        *,
        # direct overrides (optional)
        period: Optional[int] = None,
        band: Optional[float] = None,
        pct: Optional[bool] = None,
        compare_to_signal: Optional[bool] = None,
        signal: Optional[int] = None,
        edge_only: Optional[bool] = None,
        stop_pct: Optional[float] = None,
        tgt_pct: Optional[float] = None,
        # grid-search style
        timeframe: Optional[str] = None,
        params_by_tf: Optional[Dict[str, Dict[str, Any]]] = None,
        # passthrough params dict
        params: Optional[dict] = None,
        **base_kwargs,
    ) -> None:
        # 1) start from defaults
        merged = dict(self._DEFAULTS)

        # 2) bring in user 'params' dict if any
        if params:
            merged.update(params)

        # 3) merge per-timeframe config if provided (accept 'peiod' typo)
        if params_by_tf and timeframe is not None and timeframe in params_by_tf:
            tf_params = dict(params_by_tf[timeframe])  # copy
            if "period" not in tf_params and "peiod" in tf_params:
                tf_params["period"] = tf_params["peiod"]
            merged.update(tf_params)

        # 4) explicit kwargs override everything
        if period is not None:            merged["period"] = int(period)
        if band is not None:              merged["band"] = float(band)
        if pct is not None:               merged["pct"] = bool(pct)
        if compare_to_signal is not None: merged["compare_to_signal"] = bool(compare_to_signal)
        if signal is not None:            merged["signal"] = int(signal)
        if edge_only is not None:         merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:          merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:           merged["tgt_pct"] = float(tgt_pct)

        # normalize minimal constraints
        merged["period"] = max(1, int(merged.get("period", self._DEFAULTS["period"])))
        merged["band"] = max(0.0, float(merged.get("band", 0.0)))
        merged["signal"] = max(1, int(merged.get("signal", self._DEFAULTS["signal"])))

        super().__init__(params=merged, **base_kwargs)

        n = int(self.params["period"])
        use_signal = bool(self.params["compare_to_signal"])
        sig_n = int(self.params["signal"])

        # Warmup: enough for ROC (n) plus signal seeding if enabled
        self.warmup = n + (sig_n if use_signal else 0)

        self._prev_diff = None
        self._roc_signal = None
        self._roc_seed = deque(maxlen=sig_n)  # bounded queue for seeding EMA(ROC)

    # keep external call the same
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._prev_diff = None
        self._roc_signal = None
        self._roc_seed.clear()

    # ------ strategy core ------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        n = int(self.params["period"])
        band = float(self.params["band"])
        use_pct = bool(self.params["pct"])
        use_signal = bool(self.params["compare_to_signal"])
        sig_n = int(self.params["signal"])
        edge_only = bool(self.params["edge_only"])

        if len(hist) < n + 1:
            return Signal.flat("init")

        c_now = hist[-1].c
        c_prev = hist[-(n + 1)].c
        if c_prev == 0:
            return Signal.flat("bad-prev-close")

        roc = (c_now - c_prev) / c_prev
        if use_pct:
            roc *= 100.0

        # Choose comparison: zero-line or EMA(ROC)
        if use_signal:
            if self._roc_signal is None:
                # Seed EMA(ROC) with SMA over the first 'sig_n' ROC values
                self._roc_seed.append(roc)
                if len(self._roc_seed) < sig_n:
                    return Signal.flat("init-signal-seed")
                if len(self._roc_seed) == sig_n:
                    self._roc_signal = sum(self._roc_seed) / sig_n
                    diff = roc - self._roc_signal
                    self._prev_diff = diff
                    return Signal.flat("baseline")
            else:
                self._roc_signal = self._update_ema(self._roc_signal, roc, sig_n)
            diff = roc - self._roc_signal
        else:
            diff = roc  # zero-line comparison

        # first bar after diff is defined
        if self._prev_diff is None:
            self._prev_diff = diff
            return Signal.flat("baseline")

        # Deadband thresholds
        hi = +band
        lo = -band

        # Edge detection across deadband (prevents chatter inside [-band, +band])
        crossed_up = (self._prev_diff <= lo) and (diff > hi)
        crossed_dn = (self._prev_diff >= hi) and (diff < lo)
        self._prev_diff = diff

        # Optional risk targets
        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)
        stop_long = (c_now * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (c_now * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (c_now * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (c_now * (1 - tgt_pct))  if tgt_pct  > 0 else None

        if edge_only:
            if crossed_up:
                return Signal.buy(confidence=0.75, stop=stop_long, target=tgt_long,
                                  reason=("ROC↑sig" if use_signal else "ROC↑0") + f" band={band}")
            if crossed_dn:
                return Signal.sell(confidence=0.75, stop=stop_short, target=tgt_short,
                                   reason=("ROC↓sig" if use_signal else "ROC↓0") + f" band={band}")
            return Signal.flat("no-cross")

        # Non-edge (state): only act outside the deadband
        if diff > hi:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long,
                              reason=("ROC>sig" if use_signal else "ROC>0") + f" band={band}")
        elif diff < lo:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short,
                               reason=("ROC<sig" if use_signal else "ROC<0") + f" band={band}")
        else:
            return Signal.flat("in-band")

    @staticmethod
    def _update_ema(prev: float, value: float, n: int) -> float:
        alpha = 2.0 / (n + 1.0)
        return prev + alpha * (value - prev)
