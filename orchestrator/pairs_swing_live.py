"""
orchestrator/pairs_swing_live.py

Polling-based swing runner that consumes the swing pairs YAML and hydrates price
history directly from Zerodha (Kite) to emit ENTER/EXIT signals – optionally
placing CNC market orders without touching the intraday Kafka stack.
"""
from __future__ import annotations

import argparse
import math
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List, Optional, Tuple

import yaml

from libs.zerodha_history import KiteHistoricalClient, interval_for_minutes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live swing runner using Zerodha historical candles.")
    parser.add_argument("--pairs", type=str, default="configs/pairs_swing.yaml")
    parser.add_argument("--poll-sec", type=int, default=60, help="Loop interval in seconds.")
    parser.add_argument("--lookback-bars", type=int, default=220, help="Bars to hydrate per poll (per timeframe).")
    parser.add_argument("--place-orders", action="store_true", help="Place Zerodha CNC market orders on signals.")
    parser.add_argument("--product", type=str, default="CNC", help="Zerodha product type when placing orders.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def load_pairs(path: str) -> List[Dict[str, object]]:
    doc = yaml.safe_load(open(path)) or {}
    return list(doc.get("selections") or [])


@dataclass
class PairConfig:
    leg_a: str
    leg_b: str
    tf_minutes: int
    params: Dict[str, float]
    pair_id: str


class SwingEngine:
    def __init__(self, cfg: PairConfig):
        self.cfg = cfg
        params = cfg.params
        self.lookback = int(params.get("lookback", 160))
        self.beta_lookback = int(params.get("beta_lookback", self.lookback))
        self.entry_z = float(params.get("entry_z", 2.0))
        self.exit_z = float(params.get("exit_z", 1.0))
        self.stop_z = float(params.get("stop_z", 3.0))
        self.entry_fresh = int(params.get("entry_fresh_bars", 1))
        self.max_hold_min = int(params.get("max_hold_min", 72 * 60))
        self.notional_leg = float(params.get("notional_per_leg", 50_000.0))

        window = max(self.lookback, self.beta_lookback, 10)
        self.log_a: Deque[float] = deque(maxlen=window)
        self.log_b: Deque[float] = deque(maxlen=window)
        self.spreads: Deque[float] = deque(maxlen=self.lookback)
        self.last_ts: Optional[int] = None
        self.last_z: Optional[float] = None
        self.pending: Optional[Tuple[int, int]] = None  # (direction, bars_since)
        self.position = 0
        self.entry_state: Optional[Tuple[int, float, float]] = None  # ts, px_a, px_b

    def process(self, ts: int, px_a: float, px_b: float) -> Optional[Dict[str, object]]:
        if px_a <= 0 or px_b <= 0:
            return None
        if self.last_ts is not None and ts <= self.last_ts:
            return None

        la = math.log(px_a)
        lb = math.log(px_b)
        self.log_a.append(la)
        self.log_b.append(lb)
        if len(self.log_a) < 5 or len(self.log_b) < 5:
            self.last_ts = ts
            return None

        beta = self._current_beta()
        spread = la - beta * lb
        self.spreads.append(spread)
        z = self._zscore()
        signal = None

        if z is not None:
            if self.position == 0:
                signal = self._check_entry(ts, px_a, px_b, z)
            else:
                signal = self._check_exit(ts, px_a, px_b, z)
        self.last_z = z
        self.last_ts = ts
        return signal

    def _current_beta(self) -> float:
        hist = list(self.log_a)
        hist_b = list(self.log_b)
        if len(hist) < 5:
            return 1.0
        if len(hist) > self.beta_lookback:
            hist = hist[-self.beta_lookback :]
            hist_b = hist_b[-self.beta_lookback :]
        return rolling_beta(hist, hist_b)

    def _zscore(self) -> Optional[float]:
        if len(self.spreads) < 5:
            return None
        arr = list(self.spreads)
        mean = sum(arr) / len(arr)
        var = sum((v - mean) ** 2 for v in arr) / len(arr)
        sd = math.sqrt(var) if var > 1e-12 else 0.0
        if sd <= 1e-9:
            return None
        return float((arr[-1] - mean) / sd)

    def _check_entry(self, ts: int, px_a: float, px_b: float, z: float) -> Optional[Dict[str, object]]:
        prev_z = self.last_z
        if prev_z is not None:
            crossed_upper = prev_z < self.entry_z <= z
            crossed_lower = prev_z > -self.entry_z >= z
            if crossed_upper:
                self.pending = (-1, 0)
            elif crossed_lower:
                self.pending = (1, 0)
        if self.pending and self.pending[1] > self.entry_fresh:
            self.pending = None
        if self.pending:
            direction, since = self.pending
            if direction == -1 and z >= self.entry_z:
                self.position = -1
                self.entry_state = (ts, px_a, px_b)
                self.pending = None
                return {
                    "type": "ENTER",
                    "direction": -1,
                    "ts": ts,
                    "pxA": px_a,
                    "pxB": px_b,
                    "z": z,
                }
            if direction == 1 and z <= -self.entry_z:
                self.position = 1
                self.entry_state = (ts, px_a, px_b)
                self.pending = None
                return {
                    "type": "ENTER",
                    "direction": 1,
                    "ts": ts,
                    "pxA": px_a,
                    "pxB": px_b,
                    "z": z,
                }
            self.pending = (direction, since + 1)
        return None

    def _check_exit(self, ts: int, px_a: float, px_b: float, z: float) -> Optional[Dict[str, object]]:
        hold_reason = None
        if abs(z) <= self.exit_z:
            hold_reason = "revert"
        elif abs(z) >= self.stop_z:
            hold_reason = "stop"
        elif self.entry_state:
            elapsed_sec = ts - self.entry_state[0]
            if elapsed_sec >= self.max_hold_min * 60:
                hold_reason = "timeout"
        if hold_reason:
            entry_ts, entry_a, entry_b = self.entry_state or (ts, px_a, px_b)
            pnl = compute_pnl(self.position, entry_a, entry_b, px_a, px_b, self.notional_leg)
            direction = self.position
            self.position = 0
            self.entry_state = None
            self.pending = None
            return {
                "type": "EXIT",
                "direction": direction,
                "ts": ts,
                "pxA": px_a,
                "pxB": px_b,
                "reason": hold_reason,
                "pnl": pnl,
            }
        return None


def rolling_beta(xs: List[float], ys: List[float]) -> float:
    import numpy as np

    arr_x = np.array(xs, dtype=np.float64)
    arr_y = np.array(ys, dtype=np.float64)
    arr_x1 = np.vstack([np.ones_like(arr_x), arr_x]).T
    beta = np.linalg.lstsq(arr_x1, arr_y, rcond=None)[0][1]
    return float(beta)


def compute_pnl(direction: int, entry_a: float, entry_b: float, exit_a: float, exit_b: float, notional: float) -> float:
    qty_a = max(1.0, notional / max(entry_a, 1e-6))
    qty_b = max(1.0, notional / max(entry_b, 1e-6))
    if direction == -1:
        pnl_a = (entry_a - exit_a) * qty_a
        pnl_b = (exit_b - entry_b) * qty_b
    else:
        pnl_a = (exit_a - entry_a) * qty_a
        pnl_b = (entry_b - exit_b) * qty_b
    return float(pnl_a + pnl_b)


class SwingRunner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.client = KiteHistoricalClient()
        raw_pairs = load_pairs(args.pairs)
        if not raw_pairs:
            raise RuntimeError(f"No pairs found in {args.pairs}")
        self.pairs = [self._build_config(row) for row in raw_pairs]
        self.engines = {cfg.pair_id: SwingEngine(cfg) for cfg in self.pairs}
        self.positions: Dict[str, int] = {}
        self._stop = False
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

    def _handle_stop(self, signum, _frame):
        print(f"[swing-live] received {signal.Signals(signum).name}; stopping…")
        self._stop = True

    def _build_config(self, row: Dict[str, object]) -> PairConfig:
        symbol = str(row.get("symbol") or "")
        if "-" not in symbol:
            raise ValueError(f"Invalid pair symbol {symbol!r}")
        leg_a, leg_b = [s.strip().upper() for s in symbol.split("-", 1)]
        tf_minutes = int(row.get("timeframe") or 0)
        params = dict(row.get("params") or {})
        pair_id = f"{leg_a}_{leg_b}_{tf_minutes}m"
        return PairConfig(leg_a=leg_a, leg_b=leg_b, tf_minutes=tf_minutes, params=params, pair_id=pair_id)

    def loop(self) -> None:
        poll = max(15, self.args.poll_sec)
        lookback_bars = max(50, self.args.lookback_bars)
        print(f"[swing-live] tracking {len(self.pairs)} pairs poll={poll}s lookback_bars={lookback_bars}")
        while not self._stop:
            start = time.time()
            for cfg in self.pairs:
                try:
                    self._process_pair(cfg, lookback_bars)
                except Exception as exc:
                    print(f"[swing-live] error processing {cfg.pair_id}: {exc}", file=sys.stderr)
            elapsed = time.time() - start
            sleep_for = max(1.0, poll - elapsed)
            if self.args.verbose:
                print(f"[swing-live] loop took {elapsed:.2f}s; sleeping {sleep_for:.2f}s")
            time.sleep(sleep_for)

    def _process_pair(self, cfg: PairConfig, lookback_bars: int) -> None:
        engine = self.engines[cfg.pair_id]
        tf_minutes = cfg.tf_minutes
        span_minutes = tf_minutes * (lookback_bars + 5)
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(minutes=span_minutes)
        interval = interval_for_minutes(tf_minutes)
        candles_a = self.client.fetch_interval(cfg.leg_a, interval=interval, start=start, end=end)
        candles_b = self.client.fetch_interval(cfg.leg_b, interval=interval, start=start, end=end)
        history_a = [(int(_ensure_ts(row["date"]).timestamp()), float(row["close"])) for row in candles_a]
        history_b = [(int(_ensure_ts(row["date"]).timestamp()), float(row["close"])) for row in candles_b]
        map_a = {ts: px for ts, px in history_a}
        map_b = {ts: px for ts, px in history_b}
        common = sorted(ts for ts in map_a.keys() & map_b.keys())
        for ts in common:
            if engine.last_ts is not None and ts <= engine.last_ts:
                continue
            signal_payload = engine.process(ts, map_a[ts], map_b[ts])
            if signal_payload:
                self._handle_signal(cfg, signal_payload)

    def _handle_signal(self, cfg: PairConfig, payload: Dict[str, object]) -> None:
        ts = payload["ts"]
        ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))
        action = payload["type"]
        direction = payload.get("direction")
        px_a = payload.get("pxA")
        px_b = payload.get("pxB")
        if action == "ENTER":
            self.positions[cfg.pair_id] = int(direction or 0)
            print(
                f"[swing-live] ENTER {cfg.pair_id} dir={direction} "
                f"pxA={px_a:.2f} pxB={px_b:.2f} ts={ts_dt:%Y-%m-%d %H:%M} z={payload.get('z', 0):.2f}"
            )
            if self.args.place_orders:
                self._place_pair_orders(cfg, direction=int(direction or 0), px_a=px_a, px_b=px_b)
        elif action == "EXIT":
            prev = self.positions.pop(cfg.pair_id, 0)
            pnl = payload.get("pnl", 0.0)
            reason = payload.get("reason")
            print(
                f"[swing-live] EXIT {cfg.pair_id} prev_dir={prev} pxA={px_a:.2f} pxB={px_b:.2f} "
                f"reason={reason} pnl={pnl:.2f} ts={ts_dt:%Y-%m-%d %H:%M}"
            )
            if self.args.place_orders:
                self._flatten_pair(cfg, prev, px_a=px_a, px_b=px_b)

    def _place_pair_orders(self, cfg: PairConfig, *, direction: int, px_a: float, px_b: float) -> None:
        qty_a = max(1, int(cfg.params.get("notional_per_leg", 50_000.0) / max(px_a, 1e-6)))
        qty_b = max(1, int(cfg.params.get("notional_per_leg", 50_000.0) / max(px_b, 1e-6)))
        if direction == -1:
            orders = [
                (cfg.leg_a, "SELL", qty_a),
                (cfg.leg_b, "BUY", qty_b),
            ]
        else:
            orders = [
                (cfg.leg_a, "BUY", qty_a),
                (cfg.leg_b, "SELL", qty_b),
            ]
        self._submit_orders(orders)

    def _flatten_pair(self, cfg: PairConfig, prev_direction: int, *, px_a: float, px_b: float) -> None:
        qty_a = max(1, int(cfg.params.get("notional_per_leg", 50_000.0) / max(px_a, 1e-6)))
        qty_b = max(1, int(cfg.params.get("notional_per_leg", 50_000.0) / max(px_b, 1e-6)))
        if prev_direction == -1:
            orders = [
                (cfg.leg_a, "BUY", qty_a),
                (cfg.leg_b, "SELL", qty_b),
            ]
        else:
            orders = [
                (cfg.leg_a, "SELL", qty_a),
                (cfg.leg_b, "BUY", qty_b),
            ]
        self._submit_orders(orders)

    def _submit_orders(self, orders: List[Tuple[str, str, int]]) -> None:
        kite = self.client.kite
        for symbol, side, qty in orders:
            try:
                kite.place_order(
                    variety="regular",
                    exchange="NSE",
                    tradingsymbol=symbol,
                    transaction_type=side,
                    quantity=int(qty),
                    product=self.args.product,
                    order_type="MARKET",
                )
                print(f"[swing-live] placed {side} {symbol} qty={qty}")
            except Exception as exc:
                print(f"[swing-live] order failed {symbol} {side} qty={qty}: {exc}", file=sys.stderr)


def _ensure_ts(value) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    raise TypeError("Unexpected datetime payload")


def main() -> None:
    args = parse_args()
    runner = SwingRunner(args)
    runner.loop()


if __name__ == "__main__":
    main()
