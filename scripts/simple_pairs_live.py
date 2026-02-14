#!/usr/bin/env python3
"""
scripts/simple_pairs_live.py

Dead-simple pairs runner:
  * Reads configs/pairs_next_day.yaml
  * Hydrates spread stats from Zerodha
  * Polls Zerodha REST for every new candle
  * Prints each candle's z-score (current + previous)
  * Places market orders immediately when z re-enters the entry band

No Kafka, OMS, or Streamlit. Requires a valid Zerodha access token JSON
at ingestion/auth/token.json (same as the rest of the repo).
"""
from __future__ import annotations

import math
import os
import csv
import time
import platform
import shutil
import subprocess
from dataclasses import dataclass
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Deque, List, Optional, Tuple

import yaml
from dotenv import load_dotenv
from kiteconnect import KiteConnect

IST = timezone(timedelta(hours=5, minutes=30))
TOKENS_CSV = os.getenv("ZERODHA_TOKENS_CSV", "configs/tokens.csv")
PAIRS_YAML = os.getenv("PAIRS_NEXT_DAY", "configs/pairs_next_day.yaml")
TOKEN_FILE = os.getenv("ZERODHA_TOKEN_FILE", "ingestion/auth/token.json")
KITE_PRODUCT = os.getenv("KITE_PRODUCT", "MIS")
KITE_VARIETY = os.getenv("KITE_VARIETY", "regular")
KITE_EXCHANGE = os.getenv("KITE_EXCHANGE", "NSE")
KITE_VALIDITY = os.getenv("KITE_VALIDITY", "DAY")
LEG_NOTIONAL_FALLBACK = float(os.getenv("SIMPLE_NOTIONAL_PER_LEG", "0"))
PAIR_BASE_NOTIONAL = float(os.getenv("SIMPLE_PAIR_BASE_NOTIONAL", "200000"))
PAIR_LEVERAGE = float(os.getenv("SIMPLE_PAIR_LEVERAGE", "5"))
PAIR_GROSS_OVERRIDE = float(os.getenv("SIMPLE_PAIR_GROSS_NOTIONAL", "0"))
REST_DELAY_SEC = float(os.getenv("SIMPLE_KITE_INTERVAL_SEC", "0.35"))  # ~3 rps
HYDRATE_MULT = float(os.getenv("SIMPLE_HYDRATE_MULT", "3.0"))
_LAST_REST_CALL = 0.0


def ist_now() -> datetime:
    return datetime.now(tz=IST)


def to_unix(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int(ts.timestamp())


def load_tokens(path: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            sym = (row.get("tradingsymbol") or "").strip().upper()
            token = row.get("instrument_token")
            if sym and token:
                out[sym] = int(token)
    if not out:
        raise SystemExit(f"[simple] tokens csv {path} empty")
    return out


@dataclass
class PairSpec:
    pair_id: str
    pair_symbol: str
    leg_a: str
    leg_b: str
    tf: int
    entry_z: float
    entry_lower: float
    entry_upper: float
    exit_z: float
    exit_lower: float
    exit_upper: float
    stop_z: float
    lookback: int
    beta_lookback: int


@dataclass
class PairState:
    spec: PairSpec
    tokens: Tuple[int, int]
    engine: PairEngine
    last_bucket: int
    next_due: float


def load_pairs(path: str) -> List[PairSpec]:
    doc = yaml.safe_load(open(path, "r", encoding="utf-8")) or {}
    pairs: List[PairSpec] = []
    for row in doc.get("selections", []) or []:
        sym = str(row.get("symbol") or "")
        if "-" not in sym:
            continue
        leg_a, leg_b = [s.strip().upper() for s in sym.split("-", 1)]
        tf = int(row.get("timeframe") or 3)
        params = row.get("params") or {}
        entry_z = float(params.get("entry_z", 2.0))
        entry_lower = float(params.get("entry_lower", entry_z))
        entry_upper = float(params.get("entry_upper", entry_z))
        exit_z = float(params.get("exit_z", 0.5))
        exit_lower = float(params.get("exit_lower", 0.0))
        exit_upper = float(params.get("exit_upper", exit_z))
        stop_z = float(params.get("stop_z", 3.0))
        lookback = int(params.get("lookback", 120))
        beta_lb = int(params.get("beta_lookback", lookback))
        pair_id = f"{leg_a}_{leg_b}_{tf}m"
        pairs.append(
            PairSpec(
                pair_id=pair_id,
                pair_symbol=sym,
                leg_a=leg_a,
                leg_b=leg_b,
                tf=tf,
                entry_z=entry_z,
                entry_lower=entry_lower,
                entry_upper=entry_upper,
                exit_z=exit_z,
                exit_lower=exit_lower,
                exit_upper=exit_upper,
                stop_z=stop_z,
                lookback=lookback,
                beta_lookback=beta_lb,
            )
        )
    if not pairs:
        raise SystemExit(f"[simple] no pairs in {path}")
    return pairs


def align_series(
    series_a: List[Tuple[int, float]], series_b: List[Tuple[int, float]]
) -> List[Tuple[int, float, float]]:
    out: List[Tuple[int, float, float]] = []
    i = j = 0
    na, nb = len(series_a), len(series_b)
    while i < na and j < nb:
        tsa, xa = series_a[i]
        tsb, xb = series_b[j]
        if tsa == tsb:
            out.append((tsa, xa, xb))
            i += 1
            j += 1
        elif tsa < tsb:
            i += 1
        else:
            j += 1
    return out


def bucket_ts(ts: int, tf_minutes: int) -> int:
    width = max(1, tf_minutes) * 60
    return (ts // width) * width


def close_ts(bucket: int, tf_minutes: int) -> int:
    return bucket + max(1, tf_minutes) * 60


def _rest_throttle() -> None:
    global _LAST_REST_CALL
    if REST_DELAY_SEC <= 0:
        return
    wait = REST_DELAY_SEC - (time.time() - _LAST_REST_CALL)
    if wait > 0:
        time.sleep(wait)
    _LAST_REST_CALL = time.time()


def _pair_gross_notional() -> float:
    base = PAIR_BASE_NOTIONAL
    if base <= 0:
        if LEG_NOTIONAL_FALLBACK > 0:
            base = LEG_NOTIONAL_FALLBACK * 2.0
        else:
            base = 200_000.0
    leverage = PAIR_LEVERAGE if math.isfinite(PAIR_LEVERAGE) and PAIR_LEVERAGE > 0 else 1.0
    gross = PAIR_GROSS_OVERRIDE if PAIR_GROSS_OVERRIDE > 0 else base * leverage
    return max(gross, base)


PAIR_GROSS_NOTIONAL = _pair_gross_notional()


def play_notification_sound() -> None:
    sound_env = os.getenv("SIMPLE_SOUND_FILE")
    system = platform.system().lower()

    def _spawn(player: str, sound: str) -> bool:
        if not shutil.which(player):
            return False
        try:
            subprocess.Popen(
                [player, sound],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except Exception:
            return False

    if sound_env and os.path.exists(sound_env):
        if system == "darwin" and _spawn("afplay", sound_env):
            return
        if system.startswith("linux") and (_spawn("paplay", sound_env) or _spawn("aplay", sound_env)):
            return

    if system == "darwin":
        default = "/System/Library/Sounds/Ping.aiff"
        if os.path.exists(default) and _spawn("afplay", default):
            return
    elif system.startswith("linux"):
        candidates = [
            "/usr/share/sounds/freedesktop/stereo/complete.oga",
            "/usr/share/sounds/alsa/Front_Center.wav",
        ]
        for cand in candidates:
            if os.path.exists(cand) and (_spawn("paplay", cand) or _spawn("aplay", cand)):
                return

    print("\a", end="", flush=True)


def hedged_quantities(px_a: float, px_b: float, beta: float) -> Tuple[int, int]:
    gross = max(PAIR_GROSS_NOTIONAL, 0.0)
    if gross <= 0 or px_a <= 0 or px_b <= 0:
        return 0, 0
    beta_abs = abs(beta) if math.isfinite(beta) else 1.0
    beta_abs = max(beta_abs, 1e-3)
    ratio_a = 1.0
    ratio_b = beta_abs
    unit_cost = px_a * ratio_a + px_b * ratio_b
    if unit_cost <= 0:
        return 0, 0
    scale = gross / unit_cost
    qty_a = max(1, int(scale * ratio_a))
    qty_b = max(1, int(scale * ratio_b))
    return qty_a, qty_b


def fetch_history(
    kite: KiteConnect,
    token: int,
    start: datetime,
    end: datetime,
    tf_minutes: int,
):
    interval = f"{max(1, tf_minutes)}minute"
    _rest_throttle()
    try:
        return kite.historical_data(
            instrument_token=token,
            from_date=start,
            to_date=end,
            interval=interval,
            continuous=False,
            oi=False,
        )
    except Exception as exc:
        print(f"[simple] kite fetch failed token={token} tf={tf_minutes}: {exc}")
        return []


def zscore(window: Deque[float]) -> Optional[float]:
    n = len(window)
    if n < 2:
        return None
    mean = sum(window) / n
    var = sum((v - mean) ** 2 for v in window) / n
    sd = math.sqrt(var) if var > 1e-12 else 0.0
    if sd == 0:
        return None
    return (window[-1] - mean) / sd


class PairEngine:
    def __init__(self, spec: PairSpec):
        self.spec = spec
        window = max(spec.lookback, spec.beta_lookback)
        self.log_a: Deque[float] = deque(maxlen=window)
        self.log_b: Deque[float] = deque(maxlen=window)
        self.spreads: Deque[float] = deque(maxlen=spec.lookback)
        self.last_ts: Optional[int] = None
        self.last_beta: float = 1.0
        self.last_z: Optional[float] = None
        self.position: int = 0
        self._live_primed: bool = False
        lo, hi = sorted(
            max(1e-6, v)
            for v in (
                float(spec.entry_lower),
                float(spec.entry_upper),
            )
        )
        self.entry_lower = lo
        self.entry_upper = hi
        exit_floor = max(0.0, float(spec.exit_lower))
        exit_ceiling = max(exit_floor, float(spec.exit_upper))
        self.exit_lower = exit_floor
        self.exit_upper = exit_ceiling

    def ready(self) -> bool:
        return len(self.spreads) >= max(self.spec.lookback // 2, 60)

    def _beta(self) -> float:
        n = min(len(self.log_a), self.spec.beta_lookback)
        if n < 2:
            return 1.0
        xs = list(self.log_a)[-n:]
        ys = list(self.log_b)[-n:]
        sx = sum(xs)
        sy = sum(ys)
        sxx = sum(x * x for x in xs)
        sxy = sum(x * y for x, y in zip(xs, ys))
        denom = n * sxx - sx * sx
        if abs(denom) < 1e-12:
            return 1.0
        return (n * sxy - sx * sy) / denom

    def process(self, ts: int, px_a: float, px_b: float, live: bool = False) -> Optional[Dict[str, str]]:
        log_a = math.log(px_a)
        log_b = math.log(px_b)
        self.log_a.append(log_a)
        self.log_b.append(log_b)
        beta = self._beta()
        spread = log_a - beta * log_b
        self.spreads.append(spread)
        self.last_beta = beta
        prev_z = self.last_z
        z = zscore(self.spreads)
        self.last_ts = ts
        self.last_z = z
        if z is None or not self.ready():
            return None
        if live and not self._live_primed:
            self._live_primed = True
            return None

        spec = self.spec
        if self.position == 0 and prev_z is not None:
            if prev_z >= self.entry_upper and z <= self.entry_lower:
                self.position = -1
                return {"sideA": "SELL", "sideB": "BUY"}
            if prev_z <= -self.entry_upper and z >= -self.entry_lower:
                self.position = 1
                return {"sideA": "BUY", "sideB": "SELL"}
            return None

        if self.position != 0:
            stop_hit = abs(z) >= spec.stop_z
            exit_hit = False
            if prev_z is not None:
                if self.position < 0:
                    exiting_now = self.exit_lower <= z <= self.exit_upper
                    exited_from_outside = prev_z > self.exit_upper
                    exit_hit = exited_from_outside and exiting_now
                elif self.position > 0:
                    exiting_now = (-self.exit_upper) <= z <= (-self.exit_lower)
                    exited_from_outside = prev_z < -self.exit_upper
                    exit_hit = exited_from_outside and exiting_now
            if exit_hit or stop_hit:
                prev_pos = self.position
                self.position = 0
                if prev_pos < 0:
                    return {"sideA": "BUY", "sideB": "SELL"}
                else:
                    return {"sideA": "SELL", "sideB": "BUY"}
        return None


def place_orders(
    kite: KiteConnect,
    spec: PairSpec,
    sides: Dict[str, str],
    px_a: float,
    px_b: float,
    qty_a: int,
    qty_b: int,
) -> None:
    _ = kite  # unused in paper mode, kept for signature compatibility
    orders = [
        (spec.leg_a, sides["sideA"], qty_a, px_a),
        (spec.leg_b, sides["sideB"], qty_b, px_b),
    ]
    play_notification_sound()
    for sym, side, qty, px in orders:
        notional = qty * px
        print(f"[simple] ORDER {side} {qty} {sym} @~{px:.2f} notional≈{notional:,.0f}")


def hydrate_engine(
    kite: KiteConnect,
    token_a: int,
    token_b: int,
    spec: PairSpec,
):
    lookback = int(spec.lookback * HYDRATE_MULT)
    end = ist_now()
    start = end - timedelta(minutes=lookback * spec.tf)

    data_a = fetch_history(kite, token_a, start, end, spec.tf)
    data_b = fetch_history(kite, token_b, start, end, spec.tf)
    cutoff = time.time()
    ser_a = [
        (to_unix(row["date"]), float(row["close"]))
        for row in data_a
        if row.get("close") and to_unix(row["date"]) <= cutoff
    ]
    ser_b = [
        (to_unix(row["date"]), float(row["close"]))
        for row in data_b
        if row.get("close") and to_unix(row["date"]) <= cutoff
    ]
    engine = PairEngine(spec)
    tf_minutes = max(1, spec.tf)
    last_bucket = None
    for ts, px_a, px_b in align_series(ser_a, ser_b):
        bucket = bucket_ts(ts, tf_minutes)
        if last_bucket is not None and bucket == last_bucket:
            continue
        last_bucket = bucket
        engine.process(close_ts(bucket, tf_minutes), px_a, px_b)
    if last_bucket is None:
        now_bucket = bucket_ts(int(time.time()), tf_minutes)
        last_bucket = now_bucket - tf_minutes * 60
    status = "ready" if engine.ready() else "cold"
    print(f"[simple] hydrate {spec.pair_symbol} tf={spec.tf} -> {status} bars={len(engine.spreads)}")
    return engine, last_bucket


def fetch_latest(kite: KiteConnect, token: int, spec_tf: int) -> Optional[Tuple[int, float]]:
    end = ist_now()
    start = end - timedelta(minutes=spec_tf * 2)
    data = fetch_history(kite, token, start, end, spec_tf)
    if not data:
        return None
    now = time.time()
    for row in reversed(data):
        close_val = row.get("close")
        if close_val is None:
            continue
        ts = to_unix(row["date"])
        if ts > now:
            continue
        try:
            px = float(close_val)
        except (TypeError, ValueError):
            continue
        return ts, px
    return None


def next_due_from(bucket: int, tf_minutes: int) -> float:
    return bucket + max(1, tf_minutes) * 60


def main() -> None:
    load_dotenv(".env", override=False)
    load_dotenv("infra/.env", override=False)

    tokens = load_tokens(TOKENS_CSV)
    pairs = load_pairs(PAIRS_YAML)

    token_doc = Path(TOKEN_FILE)
    if not token_doc.exists():
        raise SystemExit(f"[simple] token file missing: {TOKEN_FILE}")
    access = yaml.safe_load(token_doc.read_text()).get("access_token")
    api_key = os.getenv("KITE_API_KEY")
    if not api_key or not access:
        raise SystemExit("[simple] kite creds missing")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access)
    profile = kite.profile()
    print(f"[simple] logged in as {profile.get('user_id')} {profile.get('user_name')}")
    play_notification_sound()
    print(f"[simple] target gross per pair ≈ {PAIR_GROSS_NOTIONAL:,.0f}")

    states: List[PairState] = []
    for spec in pairs:
        tok_a = tokens.get(spec.leg_a)
        tok_b = tokens.get(spec.leg_b)
        if tok_a is None or tok_b is None:
            print(f"[simple] skip {spec.pair_symbol}: missing token")
            continue
        engine, last_bucket = hydrate_engine(kite, tok_a, tok_b, spec)
        states.append(
            PairState(
                spec=spec,
                tokens=(tok_a, tok_b),
                engine=engine,
                last_bucket=last_bucket,
                next_due=next_due_from(last_bucket, spec.tf),
            )
        )

    if not states:
        print("[simple] nothing to run; exiting")
        return

    open_trades: Dict[str, Dict[str, Any]] = {}
    MAX_TRADES = int(os.getenv("SIMPLE_MAX_TRADES", "3"))
    summary_interval = 60.0
    next_summary = time.time() + summary_interval

    while True:
        now = time.time()
        if now >= next_summary:
            print_snapshot(states, open_trades)
            next_summary = now + summary_interval
            continue

        state = min(states, key=lambda s: s.next_due)
        wait = min(state.next_due, next_summary) - now
        if wait > 0:
            time.sleep(wait)
            continue

        polled = poll_pair(kite, state, open_trades, MAX_TRADES)
        if not polled:
            state.next_due = time.time() + max(REST_DELAY_SEC * 2, 2.0)


def poll_pair(
    kite: KiteConnect,
    state: PairState,
    open_trades: Dict[str, Dict[str, Any]],
    max_trades: int,
) -> bool:
    spec = state.spec
    tok_a, tok_b = state.tokens
    latest_a = fetch_latest(kite, tok_a, spec.tf)
    latest_b = fetch_latest(kite, tok_b, spec.tf)
    if not latest_a or not latest_b:
        return False
    bucket = bucket_ts(min(latest_a[0], latest_b[0]), spec.tf)
    if state.last_bucket and bucket <= state.last_bucket:
        return False

    prev_pos = state.engine.position
    tf_minutes = max(1, spec.tf)
    close = close_ts(bucket, tf_minutes)
    now = time.time()
    if close > now:
        state.next_due = close
        return False
    signal = state.engine.process(close, latest_a[1], latest_b[1], live=True)
    curr_pos = state.engine.position
    state.last_bucket = bucket
    state.next_due = next_due_from(bucket, spec.tf)

    if not signal:
        return True

    entering = prev_pos == 0 and curr_pos != 0
    exiting = prev_pos != 0 and curr_pos == 0
    if entering and len(open_trades) >= max_trades:
        print(f"[simple] skip {spec.pair_symbol}: max trades {max_trades} reached")
        state.engine.position = prev_pos
        return True

    beta_for_size = state.engine.last_beta if state.engine.last_beta is not None else 1.0
    qty_a, qty_b = hedged_quantities(latest_a[1], latest_b[1], beta_for_size)
    if qty_a <= 0 or qty_b <= 0:
        print(f"[simple] skip {spec.pair_symbol}: unable to size qty (beta={beta_for_size:.3f})")
        state.engine.position = prev_pos
        return True

    try:
        place_orders(kite, spec, signal, latest_a[1], latest_b[1], qty_a, qty_b)
    except Exception as exc:
        print(f"[simple] order failed {spec.pair_symbol}: {exc}")
        state.engine.position = prev_pos
        return True

    entry_epoch = time.time()
    pid = spec.pair_id
    if entering or not exiting:
        open_trades[pid] = {
            "pair": spec.pair_symbol,
            "tf": f"{spec.tf}m",
            "sideA": signal["sideA"],
            "sideB": signal["sideB"],
            "entry_ts": close,
            "entry_epoch": entry_epoch,
            "qtyA": qty_a,
            "qtyB": qty_b,
        }
    if exiting:
        open_trades.pop(pid, None)
    return True


def print_snapshot(states: List[PairState], open_trades: Dict[str, Dict[str, Any]]) -> None:
    now_dt = datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[simple] ---- Liveness snapshot @ {now_dt} ----")
    state_map = {s.spec.pair_id: s for s in states}
    if open_trades:
        print("[simple] Active trades:")
        for pid, info in open_trades.items():
            state = state_map.get(pid)
            engine = state.engine if state else None
            z = engine.last_z if engine else None
            z_str = f"{z:.3f}" if isinstance(z, (int, float)) else "NA"
            tf_minutes = state.spec.tf if state else 1
            engine_last_ts = engine.last_ts if engine and engine.last_ts else None
            if engine_last_ts is None and state:
                engine_last_ts = close_ts(state.last_bucket, tf_minutes)
            if engine_last_ts:
                last_ts_str = (
                    datetime.fromtimestamp(engine_last_ts, tz=timezone.utc)
                    .astimezone(IST)
                    .strftime("%H:%M:%S")
                )
                minutes = max(0.0, (time.time() - engine_last_ts) / 60.0)
                ago_str = f"{minutes:.2f}m"
            else:
                last_ts_str = "-"
                ago_str = "NA"
            open_dur = (time.time() - info.get("entry_epoch", time.time())) / 60.0
            entry_ts = info.get("entry_ts")
            entry_str = (
                datetime.fromtimestamp(entry_ts, tz=timezone.utc)
                .astimezone(IST)
                .strftime("%H:%M:%S")
                if entry_ts
                else "-"
            )
            qty_a = int(info.get("qtyA", 0) or 0)
            qty_b = int(info.get("qtyB", 0) or 0)
            qty_str = f"{qty_a}/{qty_b}" if qty_a and qty_b else "NA"
            print(
                f"[simple] * {info['pair']:<20} {info['tf']} sides=({info['sideA']},{info['sideB']}) "
                f"qty={qty_str} entry={entry_str} open_for={open_dur:.2f}m last={last_ts_str} ago={ago_str} z={z_str}"
            )
    summary_rows = []
    for state in states:
        pid = state.spec.pair_id
        if pid in open_trades:
            continue
        engine = state.engine
        ts = engine.last_ts
        if ts is None:
            ts = close_ts(state.last_bucket, state.spec.tf)
        z = engine.last_z
        if ts:
            ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST).strftime("%H:%M:%S")
            minutes = max(0.0, (time.time() - ts) / 60.0)
            min_str = f"{minutes:.2f}m"
        else:
            ts_str = "-"
            min_str = "NA"
        z_str = f"{z:.3f}" if isinstance(z, (int, float)) else "NA"
        entry_gap = float("inf")
        if isinstance(z, (int, float)):
            entry_edge = min(engine.spec.entry_lower, engine.spec.entry_upper)
            entry_gap = min(abs(z - entry_edge), abs(z + entry_edge))
        summary_rows.append((entry_gap, state.spec, z_str, ts_str, min_str))
    summary_rows.sort(key=lambda x: x[0])
    if summary_rows:
        print("[simple] Waiting pairs:")
    for gap, spec, z_str, ts_str, min_str in summary_rows:
        gap_str = f"{gap:.3f}" if math.isfinite(gap) else "NA"
        print(
            f"[simple] {spec.pair_symbol:<20} {spec.tf:>2}m z={z_str:>8} last={ts_str} ago={min_str} entry_gap={gap_str}"
        )
    print("[simple] --------------------------------------\n")


if __name__ == "__main__":
    main()
