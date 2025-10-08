# strategy/strategies/ensemble_kofn.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from collections import deque

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["EnsembleKofN"]


class EnsembleKofN(BaseStrategy):
    """
    Ensemble voting: RSI, EMA slope, MACD histogram, ROC sign.

    Votes:
      1) RSI:   rsi < rsi_lb => +1 up ; rsi > rsi_ub => +1 down  (Wilder, seeded)
      2) EMAΔ:  ema_today > ema_yesterday => +1 up ; else +1 down
      3) MACD:  (EMA_fast - EMA_slow) - EMA_signal > 0 => +1 up ; <0 => +1 down (seeded)
      4) ROC:   (close / close[-roc_lb] - 1) > 0 => +1 up ; <0 => +1 down

    Emits:
      - BUY  if up_votes  ≥ k and up_votes  > down_votes
      - SELL if down_votes ≥ k and down_votes > up_votes
      - else FLAT
      - If edge_only=True, signals only when crossing the k-threshold (reduces churn).

    Usage (unchanged):
        strat = EnsembleKofN(k=3, rsi_lb=30, rsi_ub=70, rsi_period=14,
                             ema_period=20, macd_fast=12, macd_slow=26, macd_signal=9,
                             roc_lb=12, edge_only=False)
        sig = strat.on_bar(candle)
    """
    name = "ENSEMBLE_KOFN"

    _DEFAULTS = dict(
        k=3,
        # RSI
        rsi_period=14,
        rsi_lb=30.0,
        rsi_ub=70.0,
        # EMA slope
        ema_period=20,
        # MACD
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        # ROC
        roc_lb=12,
        # behavior
        edge_only=False,
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # ---- internal state ----
    # RSI (Wilder)
    _rsi_p: int
    _rsi_seed: deque[float]         # first (rsi_period) deltas to seed avg gain/loss
    _rsi_avg_gain: Optional[float]
    _rsi_avg_loss: Optional[float]
    _prev_close: Optional[float]

    # EMA slope
    _ema_p: int
    _ema_seed: deque[float]
    _ema_alpha: float
    _ema: Optional[float]
    _ema_prev: Optional[float]

    # MACD
    _ema_f: Optional[float]
    _ema_s: Optional[float]
    _sig: Optional[float]
    _macd_seed: deque[float]        # first (macd_signal) MACD vals to seed signal EMA
    _alpha_f: float
    _alpha_s: float
    _alpha_sig: float

    # ROC
    _roc_lb: int
    _roc_q: deque[float]

    # Edge detector
    _prev_up: Optional[int]
    _prev_dn: Optional[int]

    def __init__(
        self,
        *,
        # direct overrides
        k: Optional[int] = None,
        rsi_lb: Optional[float] = None,
        rsi_ub: Optional[float] = None,
        rsi_period: Optional[int] = None,
        ema_period: Optional[int] = None,
        macd_fast: Optional[int] = None,
        macd_slow: Optional[int] = None,
        macd_signal: Optional[int] = None,
        roc_lb: Optional[int] = None,
        edge_only: Optional[bool] = None,
        stop_pct: Optional[float] = None,
        tgt_pct: Optional[float] = None,
        # grid style (optional)
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
            tfp = dict(params_by_tf[timeframe])
            merged.update(tfp)

        # explicit kw overrides
        if k is not None:              merged["k"] = int(k)
        if rsi_lb is not None:         merged["rsi_lb"] = float(rsi_lb)
        if rsi_ub is not None:         merged["rsi_ub"] = float(rsi_ub)
        if rsi_period is not None:     merged["rsi_period"] = int(rsi_period)
        if ema_period is not None:     merged["ema_period"] = int(ema_period)
        if macd_fast is not None:      merged["macd_fast"] = int(macd_fast)
        if macd_slow is not None:      merged["macd_slow"] = int(macd_slow)
        if macd_signal is not None:    merged["macd_signal"] = int(macd_signal)
        if roc_lb is not None:         merged["roc_lb"] = int(roc_lb)
        if edge_only is not None:      merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:       merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:        merged["tgt_pct"] = float(tgt_pct)

        # normalize
        merged["k"] = max(1, int(merged.get("k", 3)))
        merged["rsi_period"] = max(2, int(merged.get("rsi_period", 14)))
        merged["ema_period"] = max(2, int(merged.get("ema_period", 20)))
        merged["macd_fast"] = max(2, int(merged.get("macd_fast", 12)))
        merged["macd_slow"] = max(merged["macd_fast"] + 1, int(merged.get("macd_slow", 26)))
        merged["macd_signal"] = max(1, int(merged.get("macd_signal", 9)))
        merged["roc_lb"] = max(1, int(merged.get("roc_lb", 12)))

        super().__init__(params=merged, **base_kwargs)

        # warmup (loose upper bound so the first decision isn't biased)
        self.warmup = max(
            self.params["rsi_period"],
            self.params["ema_period"],
            self.params["macd_slow"] + self.params["macd_signal"],
            self.params["roc_lb"] + 1,
        )

        # RSI
        self._rsi_p = int(self.params["rsi_period"])
        self._rsi_seed = deque(maxlen=self._rsi_p)
        self._rsi_avg_gain = None
        self._rsi_avg_loss = None
        self._prev_close = None
        self._rsi_lb = float(self.params["rsi_lb"])
        self._rsi_ub = float(self.params["rsi_ub"])

        # EMA slope
        self._ema_p = int(self.params["ema_period"])
        self._ema_seed = deque(maxlen=self._ema_p)
        self._ema_alpha = 2.0 / (self._ema_p + 1.0)
        self._ema = None
        self._ema_prev = None

        # MACD
        f = int(self.params["macd_fast"])
        s = int(self.params["macd_slow"])
        sg = int(self.params["macd_signal"])
        self._alpha_f = 2.0 / (f + 1.0)
        self._alpha_s = 2.0 / (s + 1.0)
        self._alpha_sig = 2.0 / (sg + 1.0)
        self._ema_f = None
        self._ema_s = None
        self._sig = None
        self._macd_seed = deque(maxlen=sg)

        # ROC
        self._roc_lb = int(self.params["roc_lb"])
        self._roc_q = deque(maxlen=self._roc_lb + 1)

        # edge detector
        self._prev_up = None
        self._prev_dn = None

    # keep external interaction the same
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._rsi_seed.clear()
        self._rsi_avg_gain = None
        self._rsi_avg_loss = None
        self._prev_close = None
        self._ema_seed.clear()
        self._ema = None
        self._ema_prev = None
        self._ema_f = None
        self._ema_s = None
        self._sig = None
        self._macd_seed.clear()
        self._roc_q.clear()
        self._prev_up = None
        self._prev_dn = None

    # ---------- core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        k = int(self.params["k"])
        edge_only = bool(self.params["edge_only"])

        up, dn = 0, 0

        # ----- RSI vote (Wilder with SMA seeding) -----
        rsi = self._update_rsi(bar.c)
        if rsi is not None:
            if rsi < self._rsi_lb: up += 1
            elif rsi > self._rsi_ub: dn += 1

        # ----- EMA slope vote (EMA seeded by SMA) -----
        self._ema_prev = self._ema
        self._ema = self._update_seeded_ema(self._ema, self._ema_seed, bar.c, self._ema_alpha)
        if self._ema_prev is not None and self._ema is not None:
            if self._ema > self._ema_prev: up += 1
            elif self._ema < self._ema_prev: dn += 1

        # ----- MACD histogram vote (seed fast/slow & signal) -----
        self._ema_f = self._update_ema(self._ema_f, bar.c, self._alpha_f)
        self._ema_s = self._update_ema(self._ema_s, bar.c, self._alpha_s)
        hist_val = None
        if self._ema_f is not None and self._ema_s is not None:
            macd_val = self._ema_f - self._ema_s
            if self._sig is None:
                self._macd_seed.append(macd_val)
                if len(self._macd_seed) == self._macd_seed.maxlen:
                    self._sig = sum(self._macd_seed) / len(self._macd_seed)
            else:
                self._sig = self._sig + self._alpha_sig * (macd_val - self._sig)
            if self._sig is not None:
                hist_val = macd_val - self._sig
                if hist_val > 0: up += 1
                elif hist_val < 0: dn += 1

        # ----- ROC vote -----
        self._roc_q.append(bar.c)
        roc_ready = len(self._roc_q) > self._roc_lb
        if roc_ready:
            base = self._roc_q[0]
            if base != 0.0:
                roc = (bar.c - base) / base * 100.0
                if roc > 0: up += 1
                elif roc < 0: dn += 1
            # keep exactly (roc_lb+1) samples
            while len(self._roc_q) > (self._roc_lb + 1):
                self._roc_q.popleft()

        # ----- Decision -----
        if not self.ready():
            return Signal.flat("warmup")

        reason = f"votes_up={up},votes_dn={dn}"
        # Edge-only: emit only when crossing the k threshold with a majority
        if edge_only:
            fired = None
            prev_up = self._prev_up or 0
            prev_dn = self._prev_dn or 0
            if (up >= k and up > dn and prev_up < k):
                fired = "BUY"
            elif (dn >= k and dn > up and prev_dn < k):
                fired = "SELL"

            self._prev_up, self._prev_dn = up, dn
            if fired == "BUY":
                return Signal.buy(confidence=min(1.0, up / 4.0), reason=f"ensemble_up {reason}")
            if fired == "SELL":
                return Signal.sell(confidence=min(1.0, dn / 4.0), reason=f"ensemble_dn {reason}")
            return Signal.flat("no-edge")

        # State-based (continuous stance when majority ≥ k)
        self._prev_up, self._prev_dn = up, dn
        if up >= k and up > dn:
            return Signal.buy(confidence=min(1.0, up / 4.0), reason=f"ensemble_up {reason}")
        if dn >= k and dn > up:
            return Signal.sell(confidence=min(1.0, dn / 4.0), reason=f"ensemble_dn {reason}")
        return Signal.flat("tie/insufficient")

    # ---------- indicators ----------
    def _update_rsi(self, close: float) -> Optional[float]:
        p = self._rsi_p
        if self._prev_close is None:
            self._prev_close = close
            return None
        delta = close - self._prev_close
        self._prev_close = close

        gain = max(delta, 0.0)
        loss = -min(delta, 0.0)

        # seed with SMA of first p deltas
        if self._rsi_avg_gain is None or self._rsi_avg_loss is None:
            self._rsi_seed.append(delta)
            if len(self._rsi_seed) < p:
                return None
            # compute initial averages
            gains = [max(d, 0.0) for d in self._rsi_seed]
            losses = [-min(d, 0.0) for d in self._rsi_seed]
            self._rsi_avg_gain = sum(gains) / p
            self._rsi_avg_loss = sum(losses) / p

        else:
            # Wilder smoothing
            self._rsi_avg_gain = (self._rsi_avg_gain * (p - 1) + gain) / p
            self._rsi_avg_loss = (self._rsi_avg_loss * (p - 1) + loss) / p

        if self._rsi_avg_loss == 0.0:
            return 100.0
        rs = self._rsi_avg_gain / self._rsi_avg_loss
        return 100.0 - 100.0 / (1.0 + rs)

    @staticmethod
    def _update_ema(prev: Optional[float], value: float, alpha: float) -> float:
        return value if prev is None else (prev + alpha * (value - prev))

    @staticmethod
    def _update_seeded_ema(prev: Optional[float], seed_buf: deque[float], value: float, alpha: float) -> float:
        # seed EMA using SMA over first N values
        seed_buf.append(value)
        n = seed_buf.maxlen or 1
        if prev is None and len(seed_buf) < n:
            return sum(seed_buf) / len(seed_buf)  # transient SMA (not yet full)
        if prev is None and len(seed_buf) == n:
            return sum(seed_buf) / n
        # normal EMA update
        return prev + alpha * (value - prev)
