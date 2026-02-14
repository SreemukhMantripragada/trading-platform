"""
analytics/pairs/swing_find_pairs.py

Discover mean-reverting swing pairs directly from Zerodha (Kite) historical data
without relying on the intraday Postgres/Kafka stack.

Example:
  python -m analytics.pairs.swing_find_pairs \
    --symbols HDFCBANK,ICICIBANK,AXISBANK,SBIN,KOTAKBANK \
    --lookback-days 180 \
    --out configs/pairs_swing.yaml
"""
from __future__ import annotations

import argparse
import itertools
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import yaml
from scipy.stats import pearsonr
from statsmodels.tsa.stattools import coint

from libs.zerodha_history import KiteHistoricalClient, interval_for_minutes

DEFAULT_TFS = (5, 15, 30, 60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find swing pairs using Zerodha historical data.")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbol universe (NSE tradingsymbols).")
    parser.add_argument("--symbols-file", type=str, default="", help="File path with one symbol per line.")
    parser.add_argument("--symbol-limit", type=int, default=40, help="Max symbols when auto-loading subscribed tokens.")
    parser.add_argument("--timeframes", type=str, default="5,15,30,60", help="Comma-separated TF minutes.")
    parser.add_argument("--lookback-days", type=int, default=180, help="History to request from Kite per symbol.")
    parser.add_argument("--min-overlap", type=int, default=400, help="Minimum common bars required per pair.")
    parser.add_argument("--min-corr", type=float, default=0.7, help="Minimum absolute correlation.")
    parser.add_argument("--max-pvalue", type=float, default=0.03, help="Maximum cointegration p-value.")
    parser.add_argument("--max-pairs", type=int, default=12, help="Pairs to keep per timeframe.")
    parser.add_argument("--out", type=str, default="configs/pairs_swing.yaml", help="Output YAML path.")
    parser.add_argument("--notional-per-leg", type=float, default=50_000.0)
    parser.add_argument("--leverage", type=float, default=3.0)
    parser.add_argument("--entry-z", type=float, default=2.0)
    parser.add_argument("--exit-z", type=float, default=1.0)
    parser.add_argument("--stop-z", type=float, default=3.0)
    parser.add_argument("--lookback-bars", type=int, default=160)
    parser.add_argument("--entry-fresh-bars", type=int, default=1)
    parser.add_argument("--max-hold-min", type=int, default=72 * 60)  # 3 trading days by default
    parser.add_argument("--risk-per-trade", type=float, default=0.01)
    parser.add_argument("--bucket", type=str, default="LOW")
    parser.add_argument("--subscribe-only", action="store_true", help="Restrict auto symbol list to tokens.csv subscribe==1")
    return parser.parse_args()


def load_symbol_list(args: argparse.Namespace, client: KiteHistoricalClient) -> List[str]:
    if args.symbols:
        return sorted({sym.strip().upper() for sym in args.symbols.split(",") if sym.strip()})
    if args.symbols_file:
        path = args.symbols_file
        with open(path) as fh:
            return sorted({line.strip().upper() for line in fh if line.strip()})
    all_symbols = client.symbols(subscribed_only=args.subscribe_only)
    if args.symbol_limit and len(all_symbols) > args.symbol_limit:
        return all_symbols[: args.symbol_limit]
    return all_symbols


def fetch_series_for_symbol(
    client: KiteHistoricalClient,
    symbol: str,
    tf_minutes: int,
    lookback_days: int,
) -> Tuple[np.ndarray, np.ndarray]:
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=lookback_days)
    interval = interval_for_minutes(tf_minutes)
    candles = client.fetch_interval(symbol, interval=interval, start=start, end=end)
    if not candles:
        return np.array([]), np.array([])
    ts = np.array([int(_ensure_datetime(c["date"]).timestamp()) for c in candles], dtype=np.int64)
    close = np.array([float(c["close"]) for c in candles], dtype=np.float64)
    return ts, close


def _ensure_datetime(value) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    raise TypeError(f"Unexpected datetime value {value!r}")


def align(ts_a: np.ndarray, px_a: np.ndarray, ts_b: np.ndarray, px_b: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    if len(ts_a) == 0 or len(ts_b) == 0:
        return np.array([]), np.array([]), np.array([])
    idx_a = {int(ts): i for i, ts in enumerate(ts_a)}
    idx_b = {int(ts): i for i, ts in enumerate(ts_b)}
    common = sorted(set(idx_a.keys()) & set(idx_b.keys()))
    if not common:
        return np.array([]), np.array([]), np.array([])
    x = np.array([px_a[idx_a[ts]] for ts in common], dtype=np.float64)
    y = np.array([px_b[idx_b[ts]] for ts in common], dtype=np.float64)
    return np.array(common, dtype=np.int64), x, y


def rolling_beta(x: np.ndarray, y: np.ndarray) -> float:
    if len(x) < 2:
        return 1.0
    x1 = np.vstack([np.ones_like(x), x]).T
    beta = np.linalg.lstsq(x1, y, rcond=None)[0][1]
    return float(beta)


def evaluate_pair(
    ts: np.ndarray,
    x: np.ndarray,
    y: np.ndarray,
    *,
    min_corr: float,
    max_pvalue: float,
) -> Optional[Dict[str, float]]:
    if len(ts) < 10:
        return None
    corr, _ = pearsonr(x, y)
    if not math.isfinite(corr) or abs(corr) < min_corr:
        return None
    try:
        coint_pval = float(coint(y, x, trend="c")[1])
    except Exception:
        return None
    if not math.isfinite(coint_pval) or coint_pval > max_pvalue:
        return None
    beta = rolling_beta(x, y)
    score = abs(corr) * (1.0 - min(0.99, coint_pval))
    return {"corr": float(corr), "pval": coint_pval, "beta": beta, "score": score}


@dataclass
class Candidate:
    leg_a: str
    leg_b: str
    tf: int
    corr: float
    pval: float
    beta: float
    score: float
    overlap: int


def discover_for_timeframe(
    symbols: Sequence[str],
    tf_minutes: int,
    *,
    client: KiteHistoricalClient,
    lookback_days: int,
    min_overlap: int,
    min_corr: float,
    max_pvalue: float,
) -> List[Candidate]:
    cache: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    for sym in symbols:
        ts, px = fetch_series_for_symbol(client, sym, tf_minutes, lookback_days)
        cache[sym] = (ts, px)

    combos = itertools.combinations(symbols, 2)
    out: List[Candidate] = []
    for leg_a, leg_b in combos:
        ts_a, px_a = cache[leg_a]
        ts_b, px_b = cache[leg_b]
        ts, x, y = align(ts_a, px_a, ts_b, px_b)
        if len(ts) < min_overlap:
            continue
        metrics = evaluate_pair(ts, x, y, min_corr=min_corr, max_pvalue=max_pvalue)
        if not metrics:
            continue
        out.append(
            Candidate(
                leg_a=leg_a,
                leg_b=leg_b,
                tf=tf_minutes,
                corr=metrics["corr"],
                pval=metrics["pval"],
                beta=metrics["beta"],
                score=metrics["score"],
                overlap=len(ts),
            )
        )
    return sorted(out, key=lambda c: c.score, reverse=True)


def build_yaml(
    grouped: Dict[int, List[Candidate]],
    *,
    args: argparse.Namespace,
) -> Dict[str, object]:
    selections = []
    base_notional = args.notional_per_leg * 2.0
    for tf_minutes, candidates in grouped.items():
        top = candidates[: args.max_pairs]
        for cand in top:
            symbol_pair = f"{cand.leg_a}-{cand.leg_b}"
            selections.append(
                {
                    "symbol": symbol_pair,
                    "strategy": "PAIRS_MEANREV",
                    "timeframe": tf_minutes,
                    "params": {
                        "lookback": args.lookback_bars,
                        "beta_mode": "dynamic",
                        "entry_z": args.entry_z,
                        "exit_z": args.exit_z,
                        "stop_z": args.stop_z,
                        "leverage": args.leverage,
                        "notional_per_leg": args.notional_per_leg,
                        "base_notional": base_notional,
                        "max_hold_min": args.max_hold_min,
                        "entry_fresh_bars": args.entry_fresh_bars,
                        "beta_lookback": args.lookback_bars,
                    },
                    "bucket": args.bucket.upper(),
                    "risk_per_trade": args.risk_per_trade,
                    "stats": {
                        "overlap": cand.overlap,
                        "corr": cand.corr,
                        "pval": cand.pval,
                        "beta": cand.beta,
                        "score": cand.score,
                    },
                }
            )
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": "swing",
            "source": "zerodha",
            "lookback_days": args.lookback_days,
            "timeframes": sorted(grouped.keys()),
        },
        "selections": selections,
    }


def main() -> None:
    args = parse_args()
    timeframes = sorted({int(tf.strip()) for tf in args.timeframes.split(",") if tf.strip()})
    client = KiteHistoricalClient()
    symbols = load_symbol_list(args, client)
    if len(symbols) < 2:
        raise SystemExit("Need at least two symbols to evaluate pairs.")
    print(f"[swing-find] symbols={symbols}")
    grouped: Dict[int, List[Candidate]] = {}
    for tf in timeframes:
        print(f"[swing-find] scanning timeframe {tf}m…")
        candidates = discover_for_timeframe(
            symbols,
            tf,
            client=client,
            lookback_days=args.lookback_days,
            min_overlap=args.min_overlap,
            min_corr=args.min_corr,
            max_pvalue=args.max_pvalue,
        )
        print(f"[swing-find] tf={tf}m candidates={len(candidates)}")
        if candidates:
            grouped[tf] = candidates
    if not grouped:
        raise SystemExit("No suitable swing pairs found.")
    doc = build_yaml(grouped, args=args)
    out_path = args.out
    with open(out_path, "w") as fh:
        yaml.safe_dump(doc, fh, sort_keys=False)
    print(f"[swing-find] wrote {len(doc['selections'])} selections to {out_path}")


if __name__ == "__main__":
    main()
