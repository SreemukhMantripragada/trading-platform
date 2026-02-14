"""
compute/pair_watch_producer.py

Live pair-monitor that:
  * reads the next-day pairs whitelist (configs/pairs_next_day.yaml),
  * hydrates each pair's rolling stats directly from Zerodha Kite historical data,
  * consumes the matching Kafka bar topics (bars.3m, bars.5m, …),
  * emits ENTER/EXIT signals with hydrated z-scores to Kafka topic pairs.signals.

Signals follow the schema expected by execution/pairs_executor.py:
  {
    "pair_id": "A_B_15m",
    "pair_symbol": "A-B",
    "a_symbol": "A",
    "b_symbol": "B",
    "beta": 0.87,
    "z": 2.4,
    "action": "ENTER_SHORT_A_LONG_B",   # or ENTER_LONG_A_SHORT_B / EXIT
    "sideA": "SELL",
    "sideB": "BUY",
    "pxA": 310.5,
    "pxB": 642.1,
    "tf": 15,
    "ts": 1736755200,
    "risk_bucket": "LOW",
    "reason": "z>=entry",
  }

ENV:
  KAFKA_BROKER          (default: localhost:9092)
  OUT_TOPIC             (default: pairs.signals)
  PAIRS_NEXT_DAY        (default: configs/pairs_next_day.yaml)
  KITE_API_KEY, KITE_TOKEN_JSON, INSTRUMENTS_CSV (required for kite hydration)
  Z_ENTER / Z_EXIT / Z_STOP overrides (fallback if params missing)
  MAX_HOLD_MIN_DEFAULT  (fallback if params missing)
  HYDRATE_LOOKBACK_MULT (multiply lookback when fetching history; default 3)
  PAIR_ENTRY_RECENT_BARS (limit bars after threshold break to stay "fresh"; default 2)
  PAIRWATCH_STATE_EXPORT (default: 1 → JSON snapshots enabled)
  PAIRWATCH_STATE_PATH   (default: runs/pairwatch_state.json)
  PAIRWATCH_STATE_INTERVAL_SEC (default: 60)
  PAIRWATCH_METRICS_PORT (default: 8114)
"""
from __future__ import annotations

import asyncio
import math
import os
import csv
import sys
import shutil
import subprocess
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime, timedelta, timezone
from pathlib import Path

import ujson as json
import yaml
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

try:
    from kiteconnect import KiteConnect  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    KiteConnect = None

# --- ENV / defaults ---------------------------------------------------------

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
OUT_TOPIC = os.getenv("OUT_TOPIC", "pairs.signals")
SOURCE_MODE = os.getenv("PAIRWATCH_SOURCE_MODE", "kite_poll").lower()
TOPIC_PREFIX = os.getenv("PAIRWATCH_TOPIC_PREFIX", "bars").strip() or "bars"
POLL_BAR_COUNT = int(os.getenv("PAIRWATCH_POLL_BAR_COUNT", "3"))
MIN_POLL_WAIT = float(os.getenv("PAIRWATCH_MIN_POLL_WAIT_SEC", "0.3"))
REST_RPS = float(os.getenv("PAIRWATCH_REST_RPS", "3.0"))
PAIRS_NEXT_DAY = os.getenv("PAIRS_NEXT_DAY", "configs/pairs_next_day.yaml")

KITE_API_KEY = os.getenv("KITE_API_KEY", "r8q3kff9iwca0aw2")
KITE_TOKEN_JSON = os.getenv("KITE_TOKEN_JSON", "ingestion/auth/token.json")
INSTRUMENTS_CSV = os.getenv("INSTRUMENTS_CSV", "configs/tokens.csv")
DEFAULT_ENTRY_Z = float(os.getenv("Z_ENTER", "2.0"))
DEFAULT_EXIT_Z = float(os.getenv("Z_EXIT", "0.5"))
DEFAULT_STOP_Z = float(os.getenv("Z_STOP", "3.0"))
DEFAULT_MAX_HOLD_MIN = float(os.getenv("MAX_HOLD_MIN_DEFAULT", "300"))
HYDRATE_LOOKBACK_MULT = int(os.getenv("HYDRATE_LOOKBACK_MULT", "3"))
MIN_CANDLES_PER_PAIR = int(os.getenv("PAIRWATCH_MIN_CANDLES", "150"))
HYDRATE_LOOKBACK_MULT_RUNTIME = HYDRATE_LOOKBACK_MULT * 2
MIN_READY_POINTS = int(os.getenv("PAIR_MIN_READY_POINTS", "60"))
WARMUP_CANDLES = int(os.getenv("PAIRS_WARMUP_CANDLES", "600"))
KITE_THROTTLE_SEC = float(os.getenv("KITE_THROTTLE_SEC", "0.2"))
ENTRY_RECENT_BARS_DEFAULT = int(os.getenv("PAIR_ENTRY_RECENT_BARS", "2"))
METRICS_PORT = int(os.getenv("PAIRWATCH_METRICS_PORT", os.getenv("METRICS_PORT", "8114")))
DEFAULT_FLATTEN_HHMM = os.getenv("PAIRWATCH_FLATTEN_HHMM", "15:15")
DEFAULT_LOOKBACK = int(os.getenv("PAIRWATCH_LOOKBACK_DEFAULT", "120"))
LOOKBACK_GRID_DEFAULTS: Dict[int, List[int]] = {
    1: [4, 6, 8],
    3: [60, 90, 120],
    5: [90, 120, 150],
    15: [120, 180, 240],
}

TABLE_BY_TF = {
    1: "bars_1m",
    3: "bars_3m",
    5: "bars_5m",
    15: "bars_15m",
}

IST = timezone(timedelta(hours=5, minutes=30))


class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.interval = 1.0 / max(rate_per_sec, 1e-6)
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.perf_counter()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.perf_counter()
            self._next = now + self.interval


REST_RATE_LIMITER = RateLimiter(REST_RPS)


def parse_flatten_hhmm(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        norm = str(value).strip()
        if not norm:
            return None
        if norm.lower() in {"none", "off", "disable", "disabled", "skip"}:
            return None
        hh, mm = norm.split(":")
        h = max(0, min(23, int(hh)))
        m = max(0, min(59, int(mm)))
        return h * 60 + m
    except Exception:
        return None


def ist_minutes(ts: int) -> int:
    dt = datetime.fromtimestamp(ts, tz=IST)
    return dt.hour * 60 + dt.minute

_NOTIFY_ENABLED = os.getenv("PAIRWATCH_NOTIFY", "1").lower() not in {"0", "false", "no", "off"}
_SOUND_ENABLED = os.getenv("PAIRWATCH_NOTIFY_SOUND", "1").lower() not in {"0", "false", "no", "off"}


def _default_sound_path() -> str:
    if sys.platform == "darwin":
        return "/System/Library/Sounds/Ping.aiff"
    if sys.platform.startswith("linux"):
        # Fallback to bell if common theme files missing.
        return "bell"
    return "bell"


_SOUND_FILE = os.getenv("PAIRWATCH_SOUND_FILE", _default_sound_path())

PAIR_LABELS = ("pair_id", "pair_symbol", "tf")

PAIRWATCH_BARS_INGESTED = Counter(
    "pairwatch_bars_ingested_total",
    "Kafka bars observed by pair_watch_producer, labelled by symbol and timeframe",
    ["symbol", "tf"],
)
PAIRWATCH_BAR_LAG = Gauge(
    "pairwatch_bar_age_seconds",
    "Age in seconds of the latest bar per symbol/timeframe",
    ["symbol", "tf"],
)
PAIRWATCH_HYDRATE_SECONDS = Histogram(
    "pairwatch_hydration_seconds",
    "Time taken to hydrate a pair engine",
    buckets=(0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 40.0),
    labelnames=PAIR_LABELS,
)
PAIRWATCH_READY = Gauge(
    "pairwatch_pair_ready",
    "1 when a pair engine has sufficient history to trade",
    PAIR_LABELS,
)
PAIRWATCH_Z = Gauge(
    "pairwatch_zscore",
    "Latest spread z-score per pair",
    PAIR_LABELS,
)
PAIRWATCH_POSITION = Gauge(
    "pairwatch_position_state",
    "Current position state per pair (-1 short, 0 flat, 1 long)",
    PAIR_LABELS,
)
PAIRWATCH_LAST_TS = Gauge(
    "pairwatch_last_bar_timestamp",
    "Unix timestamp of the last processed bar per pair",
    PAIR_LABELS,
)
PAIRWATCH_STALENESS = Gauge(
    "pairwatch_last_bar_staleness_seconds",
    "Seconds since the last processed bar per pair",
    PAIR_LABELS,
)
PAIRWATCH_PRICE = Gauge(
    "pairwatch_last_price",
    "Latest observed leg price per pair",
    PAIR_LABELS + ("leg",),
)
PAIRWATCH_SIGNALS = Counter(
    "pairwatch_signals_emitted_total",
    "Signals emitted by pair_watch_producer",
    PAIR_LABELS + ("action",),
)


def _play_sound_sync() -> None:
    target = _SOUND_FILE
    if not _SOUND_ENABLED:
        return
    if target.lower() == "bell":
        print("\a", end="", flush=True)
        return
    if sys.platform == "darwin":
        player = shutil.which("afplay")
        if player and os.path.exists(target):
            subprocess.Popen([player, target], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return
    if sys.platform.startswith("linux"):
        players = ("paplay", "aplay", "play")
        for cand in players:
            player = shutil.which(cand)
            if player and os.path.exists(target):
                subprocess.Popen([player, target], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return
    # Fallback to terminal bell if no player available or path missing.
    print("\a", end="", flush=True)


def _trigger_sound() -> None:
    if not _SOUND_ENABLED:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            _play_sound_sync()
        except Exception as exc:  # pragma: no cover - best effort
            print(f"[pairwatch] sound playback failed: {exc}")
        return

    def _safe_play() -> None:
        try:
            _play_sound_sync()
        except Exception as exc:  # pragma: no cover
            print(f"[pairwatch] sound playback failed: {exc}")

    loop.run_in_executor(None, _safe_play)


def notify_signal(spec: "PairSpec", payload: Dict[str, Any], *, z: float, action: str, reason: Optional[str]) -> None:
    if not _NOTIFY_ENABLED:
        _trigger_sound()
        return
    ts = payload.get("ts")
    ts_dt = datetime.fromtimestamp(ts, tz=IST).strftime("%H:%M:%S") if ts else "unknown"
    msg = (
        f"[pairwatch] signal {spec.symbol} tf={spec.tf} action={action} "
        f"z={z:.2f} reason={reason or 'n/a'} ts={ts_dt}"
    )
    print(msg, flush=True)
    _trigger_sound()


def _pair_label_values(spec: "PairSpec") -> Tuple[str, str, str]:
    return spec.pair_id, spec.symbol, f"{spec.tf}m"


def _update_pair_gauges(
    engine: "PairEngine",
    latest_a: Optional[Tuple[int, float]],
    latest_b: Optional[Tuple[int, float]],
) -> None:
    spec = engine.spec
    labels = _pair_label_values(spec)
    ready = 1.0 if engine.ready() else 0.0
    PAIRWATCH_READY.labels(*labels).set(ready)

    z_val = engine.last_z if engine.last_z is not None and math.isfinite(engine.last_z) else 0.0
    PAIRWATCH_Z.labels(*labels).set(z_val)
    PAIRWATCH_POSITION.labels(*labels).set(float(engine.position))

    last_ts = engine.last_ts or 0
    PAIRWATCH_LAST_TS.labels(*labels).set(float(last_ts))
    if last_ts:
        staleness = max(0.0, time.time() - last_ts)
    else:
        staleness = 0.0
    PAIRWATCH_STALENESS.labels(*labels).set(staleness)

    if latest_a:
        PAIRWATCH_PRICE.labels(*labels, "A").set(float(latest_a[1]))
    else:
        PAIRWATCH_PRICE.labels(*labels, "A").set(0.0)
    if latest_b:
        PAIRWATCH_PRICE.labels(*labels, "B").set(float(latest_b[1]))
    else:
        PAIRWATCH_PRICE.labels(*labels, "B").set(0.0)


# --- helpers ----------------------------------------------------------------


def topic_for_tf(tf: int) -> str:
    return f"{TOPIC_PREFIX}.{tf}m"


def parse_symbol_pair(symbol: str) -> Tuple[str, str]:
    if "-" not in symbol:
        raise ValueError(f"Pair symbol '{symbol}' missing '-' separator")
    a, b = symbol.split("-", 1)
    return a.strip().upper(), b.strip().upper()


def ols_beta(xs: Iterable[float], ys: Iterable[float]) -> Optional[float]:
    xs_list = list(xs)
    ys_list = list(ys)
    n = len(xs_list)
    if n < 2:
        return None
    sx = sy = sxx = sxy = 0.0
    for x, y in zip(xs_list, ys_list):
        sx += x
        sy += y
        sxx += x * x
        sxy += x * y
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return None
    return (n * sxy - sx * sy) / denom


def zscore(values: Deque[float]) -> Optional[float]:
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    sd = math.sqrt(var) if var > 1e-12 else 0.0
    if sd == 0.0:
        return None
    return (values[-1] - mean) / sd


async def _fetch_latest_bar_kite(
    kite: "KiteWrap",
    symbol: str,
    tf: int,
    count: int,
) -> Optional[Tuple[int, float]]:
    await REST_RATE_LIMITER.acquire()
    if not kite.k:
        return None
    tok = _instrument_token(symbol)
    if not tok:
        print(f"[pairwatch] missing instrument token for {symbol}; skip")
        return None
    start, end = _kite_time_range(tf, count)

    def _fetch():
        return kite.k.historical_data(
            instrument_token=tok,
            from_date=start,
            to_date=end,
            interval=_kite_interval(tf),
            continuous=False,
            oi=False,
        )

    try:
        data = await asyncio.to_thread(_fetch)
    except Exception as exc:
        print(f"[pairwatch] kite poll failed for {symbol}@{tf}m: {exc}")
        return None
    if not data:
        return None
    row = data[-1]
    try:
        ts = to_unix_ts(row.get("date"))
        px = float(row.get("close") or 0.0)
    except Exception:
        return None
    if px <= 0 or ts <= 0:
        return None
    return ts, px


# --- data structures --------------------------------------------------------


@dataclass
class PairSpec:
    pair_id: str
    symbol: str
    leg_a: str
    leg_b: str
    tf: int
    lookback: int
    entry_z: float
    exit_z: float
    stop_z: float
    bucket: str
    risk_per_trade: float
    beta_mode: str = "dynamic"          # "dynamic" or "static"
    beta_lookback: int = 120
    fixed_beta: float = 1.0
    max_hold_sec: Optional[int] = None
    entry_fresh_bars: int = ENTRY_RECENT_BARS_DEFAULT
    flatten_cutoff_min: Optional[int] = None


class PairEngine:
    def __init__(self, spec: PairSpec):
        self.spec = spec
        window = max(spec.lookback, spec.beta_lookback)
        self.log_a: Deque[float] = deque(maxlen=window)
        self.log_b: Deque[float] = deque(maxlen=window)
        self.spreads: Deque[float] = deque(maxlen=spec.lookback)
        self.last_ts: Optional[int] = None
        self.position: int = 0  # 0=flat, +1=long spread, -1=short spread
        self.entry_ts: Optional[int] = None
        self.entry_reason: Optional[str] = None
        self.last_beta: float = spec.fixed_beta
        self.last_z: Optional[float] = None
        history_len = max(spec.lookback * 6, 360)
        self.z_history: Deque[Tuple[int, float]] = deque(maxlen=history_len)
        self.flatten_cutoff_min = spec.flatten_cutoff_min

    def ready(self) -> bool:
        return len(self.spreads) >= max(self.spec.lookback // 2, MIN_READY_POINTS)

    def _current_beta(self) -> float:
        if self.spec.beta_mode != "dynamic":
            return self.spec.fixed_beta
        window = min(len(self.log_a), self.spec.beta_lookback)
        if window < 2:
            return self.spec.fixed_beta
        beta = ols_beta(list(self.log_a)[-window:], list(self.log_b)[-window:])
        if beta is None or not math.isfinite(beta):
            return self.spec.fixed_beta
        return beta

    def process(self, ts: int, px_a: float, px_b: float) -> Optional[Dict[str, Any]]:
        if px_a <= 0 or px_b <= 0:
            return None

        log_a = math.log(px_a)
        log_b = math.log(px_b)
        self.log_a.append(log_a)
        self.log_b.append(log_b)

        beta = self._current_beta()
        spread = log_a - beta * log_b
        self.spreads.append(spread)
        self.last_beta = beta
        self.last_ts = ts

        z = zscore(self.spreads)
        prev_z = self.last_z
        self.last_z = z
        if z is not None:
            self.z_history.append((ts, z))
            prev_str = f"{prev_z:.3f}" if prev_z is not None else "NA"
            ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
            ts_str = ts_dt.strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"[pairwatch] {self.spec.symbol} tf={self.spec.tf}m ts={ts_str} z={z:.3f} prev={prev_str}",
                flush=True,
            )
        if z is None or not self.ready():
            return None
        flatten_cutoff = self.flatten_cutoff_min
        if flatten_cutoff is not None:
            try:
                current_min = ist_minutes(ts)
            except Exception:
                current_min = None
            beyond_flatten = current_min is not None and current_min >= flatten_cutoff
        else:
            beyond_flatten = False
        entry_z = self.spec.entry_z
        exit_z = self.spec.exit_z
        stop_z = self.spec.stop_z
        max_hold_sec = self.spec.max_hold_sec

        if self.position == 0:
            if beyond_flatten:
                return None
            if prev_z is None:
                return None
            # Enter when z-score re-enters the entry band from outside.
            crossed_short = prev_z > entry_z and z <= entry_z
            crossed_long = prev_z < -entry_z and z >= -entry_z
            if crossed_short:
                self.position = -1
                self.entry_ts = ts
                self.entry_reason = f"reenter_short_prev_{prev_z:.2f}"
                _trigger_sound()
                return {
                    "action": "ENTER_SHORT_A_LONG_B",
                    "sideA": "SELL",
                    "sideB": "BUY",
                    "beta": beta,
                    "z": z,
                    "pxA": px_a,
                    "pxB": px_b,
                    "reason": self.entry_reason,
                }
            if crossed_long:
                self.position = +1
                self.entry_ts = ts
                self.entry_reason = f"reenter_long_prev_{prev_z:.2f}"
                _trigger_sound()
                return {
                    "action": "ENTER_LONG_A_SHORT_B",
                    "sideA": "BUY",
                    "sideB": "SELL",
                    "beta": beta,
                    "z": z,
                    "pxA": px_a,
                    "pxB": px_b,
                    "reason": self.entry_reason,
                }
            return None

        # flats triggered by mean reversion / stop / timeout
        hold_reason = None
        if abs(z) <= exit_z:
            hold_reason = "revert"
        elif abs(z) >= stop_z:
            hold_reason = "stop"
        elif max_hold_sec and self.entry_ts:
            elapsed = ts - self.entry_ts
            if elapsed >= max_hold_sec:
                hold_reason = f"timeout_{elapsed//60}m"

        if hold_reason is None and beyond_flatten and self.position != 0:
            hold_reason = "flatten"

        if hold_reason:
            prev_pos = self.position
            self.position = 0
            self.entry_ts = None
            reason = hold_reason
            if prev_pos == -1:
                # short spread -> flatten: BUY A? wait mapping
                side_a = "BUY"
                side_b = "SELL"
            else:
                side_a = "SELL"
                side_b = "BUY"
            return {
                "action": "EXIT",
                "sideA": side_a,
                "sideB": side_b,
                "beta": beta,
                "z": z,
                "pxA": px_a,
                "pxB": px_b,
                "reason": reason,
            }

        return None


# --- configuration loading --------------------------------------------------


class StateExporter:
    _FALSEY = {"0", "false", "no", "off"}

    def __init__(
        self,
        engines: Dict[str, PairEngine],
        latest_bar: Dict[Tuple[str, int], Tuple[int, float]],
        *,
        interval_sec: float = 60.0,
        path: Optional[str] = None,
    ):
        self.engines = engines
        self.latest_bar = latest_bar
        try:
            configured = float(os.getenv("PAIRWATCH_STATE_INTERVAL_SEC", str(interval_sec)))
        except ValueError:
            configured = interval_sec
        self.interval = max(5.0, configured)
        default_path = path or os.getenv("PAIRWATCH_STATE_PATH", "runs/pairwatch_state.json")
        self.path = Path(default_path).expanduser()
        enabled_env = os.getenv("PAIRWATCH_STATE_EXPORT", "1").lower() not in self._FALSEY
        self.enabled = enabled_env
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        if not self.enabled:
            print("[pairwatch] state export disabled; set PAIRWATCH_STATE_EXPORT=1 to enable")
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            print(f"[pairwatch] state export mkdir failed ({self.path.parent}): {exc}")
            self.enabled = False
            return
        await self._write_once()
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._run())
        print(f"[pairwatch] state export writing to {self.path} every {int(self.interval)}s")

    async def stop(self) -> None:
        task = self._task
        if not task:
            return
        if task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.interval)
                await self._write_once()
        except asyncio.CancelledError:
            pass

    async def _write_once(self) -> None:
        snapshot = self._build_snapshot()
        await asyncio.to_thread(self._persist_snapshot, snapshot)

    def _build_snapshot(self) -> Dict[str, Any]:
        generated_at = int(time.time())
        pairs_state = []
        for pair_id in sorted(self.engines):
            engine = self.engines[pair_id]
            pairs_state.append(self._engine_state(engine))
        return {"generated_at": generated_at, "pairs": pairs_state}

    def _engine_state(self, engine: PairEngine) -> Dict[str, Any]:
        spec = engine.spec
        pending_state: Optional[Dict[str, Any]] = None

        history = [{"ts": int(ts), "z": self._safe_float(z)} for ts, z in list(engine.z_history)]
        latest_a = self._latest_price(spec.leg_a, spec.tf)
        latest_b = self._latest_price(spec.leg_b, spec.tf)
        latest_tuple_a = None
        latest_tuple_b = None
        if latest_a:
            latest_tuple_a = (latest_a["ts"], latest_a["px"])
        if latest_b:
            latest_tuple_b = (latest_b["ts"], latest_b["px"])

        _update_pair_gauges(engine, latest_tuple_a, latest_tuple_b)

        return {
            "pair_id": spec.pair_id,
            "symbol": spec.symbol,
            "tf": spec.tf,
            "leg_a": spec.leg_a,
            "leg_b": spec.leg_b,
            "ready": engine.ready(),
            "position": {1: "LONG", -1: "SHORT"}.get(engine.position, "FLAT"),
            "position_raw": engine.position,
            "last_z": self._safe_float(engine.last_z),
            "last_beta": self._safe_float(engine.last_beta),
            "last_ts": int(engine.last_ts) if engine.last_ts else None,
            "entry_ts": int(engine.entry_ts) if engine.entry_ts else None,
            "entry_reason": engine.entry_reason,
            "pending": pending_state,
            "thresholds": {
                "entry_z": self._safe_float(spec.entry_z),
                "exit_z": self._safe_float(spec.exit_z),
                "stop_z": self._safe_float(spec.stop_z),
            },
            "lookback": spec.lookback,
            "beta_mode": spec.beta_mode,
            "beta_lookback": spec.beta_lookback,
            "max_hold_sec": spec.max_hold_sec,
            "risk_bucket": spec.bucket,
            "risk_per_trade": spec.risk_per_trade,
            "points": len(engine.spreads),
            "z_history": history,
            "latest_prices": {
                spec.leg_a: latest_a,
                spec.leg_b: latest_b,
            },
        }

    def _latest_price(self, symbol: str, tf: int) -> Optional[Dict[str, Any]]:
        bar = self.latest_bar.get((symbol, tf))
        if not bar:
            return None
        ts, px = bar
        return {"ts": int(ts), "px": self._safe_float(px)}

    @staticmethod
    def _safe_float(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)) and math.isfinite(value):
            return float(value)
        return None

    def _persist_snapshot(self, snapshot: Dict[str, Any]) -> None:
        tmp_path = self.path.with_name(self.path.name + ".tmp")
        with tmp_path.open("w") as fh:
            fh.write(json.dumps(snapshot))
        os.replace(tmp_path, self.path)



def load_pairs(path: str) -> List[PairSpec]:
    if not os.path.exists(path):
        print(f"[pairwatch] YAML not found: {path}")
        return []
    doc = yaml.safe_load(open(path)) or {}
    pairs: List[PairSpec] = []
    for row in doc.get("selections", []) or []:
        try:
            leg_a, leg_b = parse_symbol_pair(str(row.get("symbol", "")))
        except ValueError as e:
            print(f"[pairwatch] skip row: {e}")
            continue
        tf = int(row.get("timeframe") or 0)
        if tf not in TABLE_BY_TF:
            print(f"[pairwatch] skip {leg_a}-{leg_b}: unsupported tf={tf}")
            continue
        params = row.get("params") or {}
        lookback_param = params.get("lookback")
        if lookback_param is None:
            tf_defaults = LOOKBACK_GRID_DEFAULTS.get(tf)
            if tf_defaults:
                mid_idx = len(tf_defaults) // 2
                lookback = int(tf_defaults[mid_idx])
            else:
                lookback = DEFAULT_LOOKBACK
        else:
            try:
                lookback = int(float(lookback_param))
            except Exception:
                lookback = DEFAULT_LOOKBACK
        lookback = max(1, lookback)
        entry_z = float(params.get("entry_z", DEFAULT_ENTRY_Z))
        exit_z = float(params.get("exit_z", DEFAULT_EXIT_Z))
        stop_z = float(params.get("stop_z", DEFAULT_STOP_Z))
        beta_mode = str(params.get("beta_mode", "dynamic")).lower()
        beta_lb_param = params.get("beta_lookback")
        try:
            beta_lookback = int(float(beta_lb_param)) if beta_lb_param is not None else lookback
        except Exception:
            beta_lookback = lookback
        fixed_beta = float(params.get("fixed_beta", 1.0))
        entry_fresh_bars = int(params.get("entry_fresh_bars", ENTRY_RECENT_BARS_DEFAULT))
        max_hold_min = params.get("max_hold_min")
        if max_hold_min is None:
            stats = row.get("stats") or {}
            avg_hold = float(stats.get("avg_hold_min", DEFAULT_MAX_HOLD_MIN))
            dynamic_floor = max(avg_hold * 2.0, DEFAULT_MAX_HOLD_MIN)
            if tf >= 60:
                # ensure swing bars can hold through at least a full trading day unless overridden in config
                dynamic_floor = max(dynamic_floor, float(tf) * 24.0)
            max_hold_min = dynamic_floor
        max_hold_sec = int(float(max_hold_min) * 60.0)
        flatten_hhmm = row.get("flatten_hhmm") or params.get("flatten_hhmm") or DEFAULT_FLATTEN_HHMM
        flatten_cutoff = parse_flatten_hhmm(str(flatten_hhmm))
        bucket = str(row.get("bucket", "MED")).upper()
        rpt = float(row.get("risk_per_trade", 0.01))
        pair_id = f"{leg_a}_{leg_b}_{tf}m"
        pairs.append(
            PairSpec(
                pair_id=pair_id,
                symbol=f"{leg_a}-{leg_b}",
                leg_a=leg_a,
                leg_b=leg_b,
                tf=tf,
                lookback=lookback,
                entry_z=entry_z,
                exit_z=exit_z,
                stop_z=stop_z,
                bucket=bucket,
                risk_per_trade=rpt,
                beta_mode="dynamic" if beta_mode != "static" else "static",
                beta_lookback=beta_lookback,
                fixed_beta=fixed_beta,
                max_hold_sec=max_hold_sec,
                entry_fresh_bars=max(entry_fresh_bars, 0),
                flatten_cutoff_min=flatten_cutoff,
            )
        )
    return pairs


# --- hydration --------------------------------------------------------------


def to_unix_ts(val: Any) -> int:
    if isinstance(val, datetime):
        dt = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(val, str):
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    raise TypeError(f"Unsupported date type for to_unix_ts: {type(val)}")


def _kite_interval(tf: int) -> str:
    tf_map = {
        1: "minute",
        3: "3minute",
        5: "5minute",
        10: "10minute",
        15: "15minute",
        30: "30minute",
        60: "60minute",
    }
    key = int(tf)
    if key not in tf_map:
        raise KeyError(f"Unsupported timeframe for Kite hydration: {tf}")
    return tf_map[key]


def _kite_time_range(tf: int, count: int) -> Tuple[datetime, datetime]:
    end_ist = datetime.now(tz=IST)
    span = count * tf + 2 * tf
    start_ist = end_ist - timedelta(minutes=span)
    return start_ist.astimezone(timezone.utc), end_ist.astimezone(timezone.utc)


def _instrument_token(symbol: str) -> Optional[int]:
    try:
        with open(INSTRUMENTS_CSV, "r", newline="") as fh:
            rdr = csv.DictReader(fh)
            best = None
            for row in rdr:
                tsym = (row.get("tradingsymbol") or "").strip().upper()
                if tsym != symbol.upper():
                    continue
                tok = row.get("instrument_token")
                if not tok:
                    continue
                sub = (row.get("subscribe") or "").strip().lower()
                if sub in ("1", "true", "yes", "y"):
                    return int(tok)
                best = int(tok)
            return best
    except Exception:
        return None


class KiteWrap:
    def __init__(self):
        self.k: Optional[KiteConnect] = None

    def connect(self) -> None:
        if not KiteConnect:
            print("[pairwatch] KiteConnect not installed; cannot hydrate from kite.")
            return
        try:
            token = None
            if os.path.exists(KITE_TOKEN_JSON):
                import json as _json

                token = _json.load(open(KITE_TOKEN_JSON)).get("access_token")
                print(token)
            if not (KITE_API_KEY and token):
                raise RuntimeError("Missing KITE_API_KEY or token JSON")
            kite = KiteConnect(api_key=KITE_API_KEY)
            kite.set_access_token(token)
            self.k = kite
            print("[pairwatch] KiteConnect ready.")
        except Exception as exc:
            self.k = None
            print(f"[pairwatch] Kite init failed: {exc}")


def fetch_kite_series(kite: KiteWrap, symbol: str, tf: int, count: int) -> List[Tuple[int, float]]:
    if not kite.k:
        return []
    tok = _instrument_token(symbol)
    if not tok:
        print(f"[pairwatch] hydrate(kite) missing token for {symbol}")
        return []
    start_utc, end_utc = _kite_time_range(tf, max(count, WARMUP_CANDLES))
    try:
        delay = max(KITE_THROTTLE_SEC * 2.0, 0.2)
        time.sleep(delay)
        data = kite.k.historical_data(
            instrument_token=tok,
            from_date=start_utc,
            to_date=end_utc,
            interval=_kite_interval(tf),
            continuous=False,
            oi=False,
        )
    except Exception as exc:
        print(f"[pairwatch] hydrate(kite) fetch failed for {symbol}: {exc}")
        return []
    out: List[Tuple[int, float]] = []
    for row in data:
        ts = to_unix_ts(row.get("date"))
        close = float(row.get("close") or 0.0)
        if close <= 0:
            continue
        out.append((ts, close))
    out.sort(key=lambda t: t[0])
    return out


def align_series(
    series_a: List[Tuple[int, float]], series_b: List[Tuple[int, float]]
) -> Iterable[Tuple[int, float, float]]:
    i = j = 0
    na, nb = len(series_a), len(series_b)
    while i < na and j < nb:
        tsa, xa = series_a[i]
        tsb, xb = series_b[j]
        if tsa == tsb:
            yield tsa, xa, xb
            i += 1
            j += 1
        elif tsa < tsb:
            i += 1
        else:
            j += 1


def hydrate_engines_kite(
    kite: KiteWrap, pairs: List[PairSpec]
) -> Tuple[Dict[str, PairEngine], Dict[Tuple[str, int], Tuple[int, float]]]:
    engines: Dict[str, PairEngine] = {}
    latest_bar: Dict[Tuple[str, int], Tuple[int, float]] = {}
    if not kite.k:
        print("[pairwatch] Kite unavailable; hydration skipped.")
        return engines, latest_bar

    pairs_by_tf: Dict[int, List[PairSpec]] = defaultdict(list)
    symbols_by_tf: Dict[int, Set[str]] = defaultdict(set)
    for spec in pairs:
        pairs_by_tf[spec.tf].append(spec)
        symbols_by_tf[spec.tf].update({spec.leg_a, spec.leg_b})

    cache: Dict[Tuple[int, str], List[Tuple[int, float]]] = {}
    for tf, symbols in symbols_by_tf.items():
        baseline = max(spec.lookback for spec in pairs_by_tf[tf]) * HYDRATE_LOOKBACK_MULT_RUNTIME
        if tf == 15:
            baseline = max(baseline, MIN_CANDLES_PER_PAIR)
        limit = max(baseline, WARMUP_CANDLES)
        for sym in symbols:
            cache[(tf, sym)] = fetch_kite_series(kite, sym, tf, limit)

    for specs in pairs_by_tf.values():
        for spec in specs:
            pair_labels = _pair_label_values(spec)
            start = time.perf_counter()
            ser_a = cache.get((spec.tf, spec.leg_a), [])
            ser_b = cache.get((spec.tf, spec.leg_b), [])
            engine = PairEngine(spec)
            for ts, px_a, px_b in align_series(ser_a, ser_b):
                engine.process(ts, px_a, px_b)
            if ser_a:
                latest_bar[(spec.leg_a, spec.tf)] = ser_a[-1]
            if ser_b:
                latest_bar[(spec.leg_b, spec.tf)] = ser_b[-1]
            engines[spec.pair_id] = engine
            status = "ready" if engine.ready() else "cold"
            print(
                f"[pairwatch] hydrate kite {spec.symbol} tf={spec.tf} -> {status} points={len(engine.spreads)}"
            )
            duration = time.perf_counter() - start
            PAIRWATCH_HYDRATE_SECONDS.labels(*pair_labels).observe(duration)
            latest_a = latest_bar.get((spec.leg_a, spec.tf))
            latest_b = latest_bar.get((spec.leg_b, spec.tf))
            _update_pair_gauges(engine, latest_a, latest_b)
    return engines, latest_bar


def build_cold_engines(pairs: List[PairSpec]) -> Tuple[Dict[str, PairEngine], Dict[Tuple[str, int], Tuple[int, float]]]:
    engines: Dict[str, PairEngine] = {}
    latest_bar: Dict[Tuple[str, int], Tuple[int, float]] = {}
    for spec in pairs:
        engines[spec.pair_id] = PairEngine(spec)
    return engines, latest_bar


# --- main -------------------------------------------------------------------


async def main():
    start_http_server(METRICS_PORT)
    pairs = load_pairs(PAIRS_NEXT_DAY)
    if not pairs:
        print("[pairwatch] no pairs loaded; exiting")
        return

    topics = sorted({topic_for_tf(spec.tf) for spec in pairs})
    print(f"[pairwatch] monitoring {len(pairs)} pairs across topics: {topics}")

    hydrate_source = (os.getenv("PAIR_HYDRATE_SOURCE", "kite") or "kite").strip().lower()
    if hydrate_source in {"none", "off", "disabled", "cold", "skip"}:
        print("[pairwatch] hydration disabled; starting cold engines")
        engines, latest_bar = build_cold_engines(pairs)
        kite = KiteWrap()
    else:
        kite = KiteWrap()
        kite.connect()
        engines, latest_bar = hydrate_engines_kite(kite, pairs)
        if not engines:
            print("[pairwatch] kite hydration failed; exiting.")
            return

    exporter = StateExporter(engines, latest_bar)
    await exporter.start()

    symbol_map: Dict[Tuple[str, int], List[PairEngine]] = defaultdict(list)
    for engine in engines.values():
        spec = engine.spec
        symbol_map[(spec.leg_a, spec.tf)].append(engine)
        symbol_map[(spec.leg_b, spec.tf)].append(engine)

    async def emit_signal(engine: PairEngine, tf: int, ts: int, producer: AIOKafkaProducer) -> None:
        spec = engine.spec
        a_bar = latest_bar.get((spec.leg_a, tf))
        b_bar = latest_bar.get((spec.leg_b, tf))
        if not a_bar or not b_bar:
            return
        ts_a, px_a = a_bar
        ts_b, px_b = b_bar
        if ts_a != ts or ts_b != ts:
            return
        if engine.last_ts is not None and ts <= engine.last_ts:
            return
        signal = engine.process(ts, px_a, px_b)
        _update_pair_gauges(engine, a_bar, b_bar)
        if not signal:
            return
        payload = {
            "pair_id": spec.pair_id,
            "pair_symbol": spec.symbol,
            "a_symbol": spec.leg_a,
            "b_symbol": spec.leg_b,
            "tf": spec.tf,
            "ts": ts,
            "beta": signal["beta"],
            "z": signal["z"],
            "action": signal["action"],
            "sideA": signal.get("sideA"),
            "sideB": signal.get("sideB"),
            "pxA": signal.get("pxA"),
            "pxB": signal.get("pxB"),
            "risk_bucket": spec.bucket,
            "risk_per_trade": spec.risk_per_trade,
            "reason": signal.get("reason"),
        }
        await producer.send_and_wait(OUT_TOPIC, json.dumps(payload).encode(), key=spec.pair_id.encode())
        action = str(signal["action"])
        PAIRWATCH_SIGNALS.labels(*_pair_label_values(spec), action).inc()
        notify_signal(
            spec,
            payload,
            z=float(signal["z"]),
            action=action,
            reason=signal.get("reason"),
        )

    producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await producer.start()

    if SOURCE_MODE == "kite_poll":
        print("[pairwatch] Zerodha poll mode enabled; skipping Kafka bars")
        _trigger_sound()

        async def poll_pair(engine: PairEngine) -> None:
            spec = engine.spec
            tf_seconds = spec.tf * 60
            last_seen = engine.last_ts or 0
            while True:
                start_loop = time.time()
                res_a, res_b = await asyncio.gather(
                    _fetch_latest_bar_kite(kite, spec.leg_a, spec.tf, POLL_BAR_COUNT),
                    _fetch_latest_bar_kite(kite, spec.leg_b, spec.tf, POLL_BAR_COUNT),
                )
                if res_a and res_b:
                    ts_a, px_a = res_a
                    ts_b, px_b = res_b
                    ts = min(ts_a, ts_b)
                    prev_a = latest_bar.get((spec.leg_a, spec.tf))
                    prev_b = latest_bar.get((spec.leg_b, spec.tf))
                    if prev_a and ts_a <= prev_a[0]:
                        res_a = None
                    if prev_b and ts_b <= prev_b[0]:
                        res_b = None
                    if res_a and res_b and ts > last_seen:
                        latest_bar[(spec.leg_a, spec.tf)] = res_a
                        latest_bar[(spec.leg_b, spec.tf)] = res_b
                        tf_label = f"{spec.tf}m"
                        PAIRWATCH_BARS_INGESTED.labels(spec.leg_a, tf_label).inc()
                        PAIRWATCH_BARS_INGESTED.labels(spec.leg_b, tf_label).inc()
                        age = max(0.0, time.time() - ts)
                        PAIRWATCH_BAR_LAG.labels(spec.leg_a, tf_label).set(age)
                        PAIRWATCH_BAR_LAG.labels(spec.leg_b, tf_label).set(age)
                        await emit_signal(engine, spec.tf, ts, producer)
                        last_seen = ts
                now = time.time()
                next_due = last_seen + tf_seconds if last_seen else now + tf_seconds
                wait = max(MIN_POLL_WAIT, min(tf_seconds / 10.0, max(MIN_POLL_WAIT, next_due - now)))
                await asyncio.sleep(wait)

        tasks = [asyncio.create_task(poll_pair(engine)) for engine in engines.values()]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await exporter.stop()
            await producer.stop()
        return

    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=BROKER,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id="pair_watch_producer",
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None,
    )

    topic_to_tf = {topic_for_tf(spec.tf): spec.tf for spec in pairs}

    await consumer.start()
    print(f"[pairwatch] IN={topics} → OUT={OUT_TOPIC}; startup complete")
    _trigger_sound()

    try:
        while True:
            batches = await consumer.getmany(timeout_ms=750, max_records=500)
            for tp, msgs in batches.items():
                tf = topic_to_tf.get(tp.topic)
                if tf is None:
                    continue
                for m in msgs:
                    r = m.value
                    sym = str(r["symbol"]).upper()
                    ts = int(r["ts"])
                    px = float(r["c"])
                    tf_label = f"{tf}m"
                    PAIRWATCH_BARS_INGESTED.labels(sym, tf_label).inc()
                    age = max(0.0, time.time() - ts)
                    PAIRWATCH_BAR_LAG.labels(sym, tf_label).set(age)
                    latest_bar[(sym, tf)] = (ts, px)
                    engines_for_symbol = symbol_map.get((sym, tf))
                    if not engines_for_symbol:
                        continue
                    for engine in engines_for_symbol:
                        await emit_signal(engine, tf, ts, producer)
            if batches:
                await consumer.commit()
    finally:
        await exporter.stop()
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
