"""
Utility helpers to pull Zerodha (Kite Connect) historical candles without relying
on the internal Postgres/Kafka pipelines.  Designed for swing tooling that needs
to hydrate price history on demand.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from kiteconnect import KiteConnect  # type: ignore
except ImportError as exc:  # pragma: no cover - optional dependency
    raise RuntimeError(
        "kiteconnect is required. Install with `pip install kiteconnect`."
    ) from exc


def _load_access_token(path: Path) -> str:
    data = json.loads(path.read_text())
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Missing access_token in {path}")
    return str(token)


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


class InstrumentResolver:
    """
    Lightweight loader for configs/tokens.csv so we can map symbols to instrument tokens.
    """

    def __init__(self, csv_path: Path):
        self._by_symbol: Dict[str, Dict[str, str]] = {}
        if not csv_path.exists():
            raise FileNotFoundError(f"tokens csv missing: {csv_path}")
        with csv_path.open(newline="") as fh:
            import csv

            reader = csv.DictReader(fh)
            for row in reader:
                sym = (row.get("tradingsymbol") or row.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                self._by_symbol[sym] = {k: (v or "").strip() for k, v in row.items()}

    def symbols(self, *, subscribed_only: bool = False) -> List[str]:
        if not subscribed_only:
            return sorted(self._by_symbol.keys())
        out = []
        for sym, row in self._by_symbol.items():
            subscribe = row.get("subscribe")
            if subscribe and _parse_bool(subscribe):
                out.append(sym)
        return sorted(out)

    def instrument_token(self, symbol: str) -> Optional[int]:
        info = self._by_symbol.get(symbol.upper())
        if not info:
            return None
        tok = info.get("instrument_token")
        if tok is None or tok == "":
            return None
        try:
            return int(tok)
        except ValueError:
            return None


class KiteHistoricalClient:
    """
    Thin wrapper around KiteConnect historical_data with request throttling and
    instrument-token resolution.
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        token_json: Optional[str] = None,
        tokens_csv: str = "configs/tokens.csv",
        throttle_sec: float = 0.3,
    ):
        api_key = api_key or os.getenv("KITE_API_KEY")
        if not api_key:
            raise RuntimeError("KITE_API_KEY missing")
        token_json_path = Path(token_json or os.getenv("KITE_TOKEN_JSON", "ingestion/auth/token.json"))
        if not token_json_path.exists():
            raise FileNotFoundError(f"KITE_TOKEN_JSON not found: {token_json_path}")

        access_token = _load_access_token(token_json_path)
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        self._kite = kite

        self._resolver = InstrumentResolver(Path(tokens_csv))
        self._throttle_sec = throttle_sec
        self._lock = threading.Lock()
        self._last_call = 0.0

    def symbols(self, *, subscribed_only: bool = False) -> List[str]:
        return self._resolver.symbols(subscribed_only=subscribed_only)

    def instrument_token(self, symbol: str) -> int:
        token = self._resolver.instrument_token(symbol)
        if token is None:
            raise RuntimeError(f"No instrument_token for {symbol}")
        return token

    @property
    def kite(self) -> KiteConnect:
        return self._kite

    def _throttle(self) -> None:
        with self._lock:
            now = time.time()
            delta = now - self._last_call
            if delta < self._throttle_sec:
                time.sleep(self._throttle_sec - delta)
            self._last_call = time.time()

    def fetch_interval(
        self,
        symbol: str,
        *,
        interval: str,
        start: datetime,
        end: datetime,
        continuous: bool = False,
    ) -> List[Dict[str, object]]:
        """
        Fetch candles for [start, end]. Kite limits the span of a single query
        to 60 days for intraday frames, so we chunk automatically.
        """
        token = self.instrument_token(symbol)
        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        if start_utc >= end_utc:
            return []
        data: List[Dict[str, object]] = []
        cursor = start_utc
        max_span = timedelta(days=60)
        while cursor < end_utc:
            chunk_end = min(cursor + max_span, end_utc)
            self._throttle()
            candles = self._kite.historical_data(
                token,
                cursor,
                chunk_end,
                interval,
                continuous=continuous,
                oi=False,
            )
            data.extend(candles)
            cursor = chunk_end
            # Avoid gaps/duplicates by advancing one microsecond beyond chunk_end
            cursor += timedelta(seconds=1)
        return data


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


INTERVAL_MAP = {
    1: "minute",
    3: "3minute",
    5: "5minute",
    10: "10minute",
    15: "15minute",
    30: "30minute",
    60: "60minute",
}


def interval_for_minutes(tf: int) -> str:
    key = INTERVAL_MAP.get(int(tf))
    if not key:
        raise ValueError(f"Unsupported timeframe minutes={tf}")
    return key
