# backtest/scorer.py
"""
Scoring and constraints for strategy configs (drop-in).

Inputs expected (per row):
- symbol, strategy, timeframe (or tf/tf_min)
- metrics (any subset is fine; missing handled):
    R (preferred, normalized) or pnl_total (absolute),
    sharpe, sortino, win_rate,
    max_dd, ulcer,
    turnover, avg_hold_min,
    stability (preferred) or weekly_win_rate,
    latency_ms (optional),
    penalties (optional, +ve is worse),
    asof/end_date/last_ts/last_bar (optional timestamps for freshness decay),
    n_trades (used for aggregation weights and gates).

Outputs:
- apply_constraints(rows, gates) -> filtered rows (adds r['rejected_reason'] when dropped)
- score_rows(rows, weights) -> rows (in place) with r['score'], sorted desc by score
- aggregate_by_config(rows) -> dict key=(strategy,timeframe,frozenset(params.items()))
- aggregate_by_symbol_config(rows) -> dict key=(symbol,strategy,timeframe,frozenset(params.items()))
- select_topk_per_symbol_tf(rows, k)
- select_topk_per_symbol_tf_strategy(rows, k)
- decide_next_day(rows, policy) -> rows with eligibility flags
- eligible_by_symbol_tf(rows, policy, limit_per_bucket)

Design choices:
- Robust z-scores computed in cohorts of (strategy × timeframe) to enable
  cross-symbol comparability for a given setup.
- Winsorization + median-impute with small missing penalty.
- Optional exponential freshness decay (weights['freshness_half_life_days']).
- Drawdown penalized by magnitude |max_dd|.
"""

from __future__ import annotations
from typing import Dict, Any, Iterable, List, Tuple, Optional, Sequence
import statistics as stats
from math import exp, log
from datetime import datetime

# ---------------- Defaults ----------------
# Hard gates are OFF unless provided by caller (keep behavior predictable).
DEFAULT_GATES: Dict[str, Optional[float]] = {
    "min_trades": None,        # e.g., 50  (set in caller)
    "min_win_rate": None,      # e.g., 0.52 for 52%
    "max_drawdown": None,      # compare to |max_dd| magnitude (same unit as your PnL normalization)
    "max_slippage_bps": None,  # if your rows include 'slippage_bps'
    "require_positive_return": True,  # prefer R, else pnl_total must be > 0
    "min_R": None,             # if you score on R
    "min_pnl_total": None,     # if you score on absolute pnl
}

# Returns-forward profile (heavier weight on R/Sharpe; softer turnover/hold penalties).
DEFAULT_WEIGHTS: Dict[str, float] = {
    "w_return":    2.5,  # ↑ emphasize total return/R strongly
    "w_sharpe":    1.2,  # ↑ risk-adjusted return matters
    "w_sortino":   0.8,  # ↑ downside risk sensitivity if present
    "w_win":       0.6,  # ↑ some weight on hit-rate
    "w_stability": 0.6,  # ↑ prefer stable weekly/rolling behavior
    "w_dd":        0.7,  # ↓ penalize large drawdowns (magnitude)
    "w_ulcer":     0.4,  # ↓ persistence of drawdowns
    "w_turnover":  0.10, # ↓ small penalty (we’re returns-first)
    "w_hold":      0.10, # ↓ small penalty for long holds if you want faster cycles
    "w_latency":   0.0,  # ↓ set >0 only if execution latency is a concern
    "w_penalty":   0.4,  # ↓ penalties field is treated as “worse is higher”
    # Optional recency decay (days half-life). Uncomment/override in caller if needed.
    # "freshness_half_life_days": 120.0,
    # Extra hit if effective return is negative (in score units)
    "neg_return_penalty": 2.0,
}

# ---------------- Utils ----------------

def _winsorize(values: Sequence[float], p: float = 0.02) -> List[float]:
    if not values:
        return []
    xs = sorted(values)
    n = len(xs)
    lo_i = max(0, int(n * p))
    hi_i = max(lo_i, int(n * (1.0 - p)) - 1)
    lo_v, hi_v = xs[lo_i], xs[hi_i]
    return [min(max(v, lo_v), hi_v) for v in values]

def _zscore_with_impute(series: List[Optional[float]], missing_penalty_z: float = 0.1) -> List[float]:
    """Winsorize present values, z-score vs winsorized median & pstdev.
       Impute missing to median and subtract a small penalty."""
    if not series:
        return []
    present = [float(v) for v in series if v is not None]
    if not present:
        return [0.0] * len(series)
    xs = _winsorize(present, 0.02)
    m = stats.median(xs)
    sd = stats.pstdev(xs) or 1.0
    out: List[float] = []
    for v in series:
        if v is None:
            out.append(0.0 - abs(missing_penalty_z))
        else:
            out.append((float(v) - m) / sd)
    return out

def _extract_metric(rows: List[Dict[str, Any]], keys: List[str]) -> List[Optional[float]]:
    """Pull the first present key from `keys` per row. Keep None if all missing."""
    out: List[Optional[float]] = []
    for r in rows:
        v = None
        for k in keys:
            if k in r and r[k] is not None:
                try:
                    v = float(r[k])
                except (TypeError, ValueError):
                    v = None
                break
        out.append(v)
    return out

def _group_key(r: Dict[str, Any]) -> Tuple[Any, Any]:
    """Cohort for normalization: (strategy × timeframe).
       This enables apples-to-apples across symbols within a given setup."""
    return (r.get("strategy"), int(r.get("timeframe") or r.get("tf") or r.get("tf_min") or 0))

def _parse_dt(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x
    s = str(x)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.utcfromtimestamp(float(s))
        except Exception:
            return None

def _freshness_weight(row: Dict[str, Any], half_life_days: Optional[float]) -> float:
    if not half_life_days:
        return 1.0
    ts = _parse_dt(row.get("asof") or row.get("end_date") or row.get("last_ts") or row.get("last_bar"))
    if not ts:
        return 1.0
    now = datetime.utcnow()
    age_days = max(0.0, (now - ts).total_seconds() / 86400.0)
    return float(exp(-log(2.0) * (age_days / float(half_life_days))))

def _effective_return_val(r: Dict[str, Any]) -> Optional[float]:
    """Prefer normalized R; else fall back to absolute pnl_total."""
    if r.get("R") is not None:
        try:
            return float(r["R"])
        except Exception:
            return None
    if r.get("pnl_total") is not None:
        try:
            return float(r["pnl_total"])
        except Exception:
            return None
    return None

# ---------------- API ----------------

def apply_constraints(rows: List[Dict[str, Any]], gates: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Hard filter rows; attaches r['rejected_reason'] for drops."""
    if not rows:
        return []
    g = {**DEFAULT_GATES, **(gates or {})}
    out: List[Dict[str, Any]] = []

    for r in rows:
        # Positive return first (strict > 0). Prefer R else pnl_total. If both missing => reject.
        if g.get("require_positive_return", True):
            eff = _effective_return_val(r)
            if eff is None or eff <= 0.0:
                r["rejected_reason"] = "non_positive_return"
                continue

        if g["min_trades"] is not None and int(r.get("n_trades", 0) or 0) < int(g["min_trades"]):
            r["rejected_reason"] = "min_trades"; continue

        if g["min_win_rate"] is not None:
            wr = float(r.get("win_rate", 0.0) or 0.0)
            wr = wr / 100.0 if wr > 1.0 else wr
            if wr < float(g["min_win_rate"]):
                r["rejected_reason"] = "min_win_rate"; continue

        if g["max_drawdown"] is not None:
            dd_mag = abs(float(r.get("max_dd", 0.0) or 0.0))
            if dd_mag > float(g["max_drawdown"]):
                r["rejected_reason"] = "max_drawdown"; continue

        if g["max_slippage_bps"] is not None and ("slippage_bps" in r):
            if float(r.get("slippage_bps", 0.0) or 0.0) > float(g["max_slippage_bps"]):
                r["rejected_reason"] = "slippage"; continue

        if g["min_R"] is not None and (r.get("R") is not None):
            try:
                if float(r["R"]) < float(g["min_R"]):
                    r["rejected_reason"] = "min_R"; continue
            except Exception:
                pass

        if g["min_pnl_total"] is not None and (r.get("pnl_total") is not None):
            try:
                if float(r["pnl_total"]) < float(g["min_pnl_total"]):
                    r["rejected_reason"] = "min_pnl_total"; continue
            except Exception:
                pass

        out.append(r)
    return out

def score_rows(rows: List[Dict[str, Any]], weights: Dict[str, float]) -> List[Dict[str, Any]]:
    """Score within (strategy × timeframe) cohorts using robust z-scores."""
    if not rows:
        return []

    wr = {**DEFAULT_WEIGHTS, **(weights or {})}
    half_life = wr.get("freshness_half_life_days")

    # Cohorts: (strategy, timeframe)
    buckets: Dict[Tuple[Any, Any], List[int]] = {}
    for i, r in enumerate(rows):
        buckets.setdefault(_group_key(r), []).append(i)

    comps = {
        "ret":   [0.0]*len(rows),
        "shp":   [0.0]*len(rows),
        "srt":   [0.0]*len(rows),
        "win":   [0.0]*len(rows),
        "dd":    [0.0]*len(rows),
        "uix":   [0.0]*len(rows),
        "tov":   [0.0]*len(rows),
        "hold":  [0.0]*len(rows),
        "stab":  [0.0]*len(rows),
        "lat":   [0.0]*len(rows),
        "pen":   [0.0]*len(rows),
    }
    eff_ret: List[Optional[float]] = [None]*len(rows)

    for key, idxs in buckets.items():
        grp = [rows[i] for i in idxs]
        ret  = _extract_metric(grp, ["R", "pnl_total"])
        shp  = _extract_metric(grp, ["sharpe"])
        srt  = _extract_metric(grp, ["sortino"])
        win  = _extract_metric(grp, ["win_rate"])
        dd   = _extract_metric(grp, ["max_dd"])
        uix  = _extract_metric(grp, ["ulcer"])
        tov  = _extract_metric(grp, ["turnover"])
        hold = _extract_metric(grp, ["avg_hold_min"])
        stab = _extract_metric(grp, ["stability", "weekly_win_rate"])
        lat  = _extract_metric(grp, ["latency_ms"])
        pen  = _extract_metric(grp, ["penalties"])

        dd_mag = [abs(x) if x is not None else None for x in dd]

        z_ret  = _zscore_with_impute(ret)
        z_shp  = _zscore_with_impute(shp)
        z_srt  = _zscore_with_impute(srt)
        z_win  = _zscore_with_impute(win)
        z_dd   = _zscore_with_impute(dd_mag)
        z_uix  = _zscore_with_impute(uix)
        z_tov  = _zscore_with_impute(tov)
        z_hold = _zscore_with_impute(hold)
        z_stab = _zscore_with_impute(stab)
        z_lat  = _zscore_with_impute(lat)
        z_pen  = _zscore_with_impute(pen)

        for pos, i in enumerate(idxs):
            comps["ret"][i]  = z_ret[pos]
            comps["shp"][i]  = z_shp[pos]
            comps["srt"][i]  = z_srt[pos]
            comps["win"][i]  = z_win[pos]
            comps["dd"][i]   = z_dd[pos]
            comps["uix"][i]  = z_uix[pos]
            comps["tov"][i]  = z_tov[pos]
            comps["hold"][i] = z_hold[pos]
            comps["stab"][i] = z_stab[pos]
            comps["lat"][i]  = z_lat[pos]
            comps["pen"][i]  = z_pen[pos]
            eff_ret[i] = _effective_return_val(grp[pos])

    for i, r in enumerate(rows):
        base = (
            wr["w_return"]    * comps["ret"][i]  +
            wr["w_sharpe"]    * comps["shp"][i]  +
            wr["w_sortino"]   * comps["srt"][i]  +
            wr["w_win"]       * comps["win"][i]  +
            wr["w_stability"] * comps["stab"][i] -
            wr["w_dd"]        * comps["dd"][i]   -
            wr["w_ulcer"]     * comps["uix"][i]  -
            wr["w_turnover"]  * comps["tov"][i]  -
            wr["w_hold"]      * comps["hold"][i] -
            wr["w_latency"]   * comps["lat"][i]  -
            wr["w_penalty"]   * comps["pen"][i]
        )
        if wr.get("neg_return_penalty", 0.0) and (eff_ret[i] is not None) and (eff_ret[i] < 0.0):
            base -= float(wr["neg_return_penalty"])
        r["score"] = float(base * _freshness_weight(r, half_life))

    rows.sort(key=lambda x: x.get("score", float("-inf")), reverse=True)
    return rows

def _wavg(vals: List[float], wts: List[float], default=0.0) -> float:
    if not vals:
        return default
    W = sum(wts)
    if W <= 0:
        return sum(vals) / len(vals)
    return sum(v * w for v, w in zip(vals, wts)) / W

def aggregate_by_config(rows: Iterable[Dict[str, Any]]) -> Dict[Tuple, Dict[str, Any]]:
    """Trade-weighted aggregation across **symbols** → per (strategy, timeframe, params)."""
    buckets: Dict[Tuple, List[Dict[str, Any]]] = {}
    for r in rows:
        key = (
            r["strategy"],
            int(r.get("timeframe") or r.get("tf") or r.get("tf_min") or 0),
            frozenset((r.get("params") or {}).items())
        )
        buckets.setdefault(key, []).append(r)

    out: Dict[Tuple, Dict[str, Any]] = {}
    for key, grp in buckets.items():
        wts = [float(x.get("n_trades", 0) or 0.0) for x in grp]
        if sum(wts) == 0:
            wts = [1.0] * len(grp)

        def vlist(k: str, default=0.0) -> List[float]:
            return [float(x.get(k, default) or default) for x in grp]

        out[key] = {
            "strategy": key[0],
            "timeframe": key[1],
            "params": dict(key[2]),
            "score": _wavg(vlist("score"), wts, 0.0),
            "pnl_total": _wavg(vlist("pnl_total"), wts, 0.0),
            "R": _wavg(vlist("R"), wts, 0.0),
            "sharpe": _wavg(vlist("sharpe"), wts, 0.0),
            "win_rate": _wavg(vlist("win_rate"), wts, 0.0),
            "max_dd": max((abs(v) for v in vlist("max_dd", 0.0)), default=0.0) if grp else 0.0,
            "n_trades": int(sum(float(x.get("n_trades", 0) or 0.0) for x in grp)),
            "symbols": sorted({x.get("symbol", "") for x in grp if "symbol" in x}),
            "variant": next((x.get("variant") for x in grp if "variant" in x), None),
        }
    return out

def aggregate_by_symbol_config(rows: Iterable[Dict[str, Any]]) -> Dict[Tuple, Dict[str, Any]]:
    """Preferred when you don’t want cross-symbol mixing."""
    buckets: Dict[Tuple, List[Dict[str, Any]]] = {}
    for r in rows:
        key = (
            r.get("symbol"),
            r["strategy"],
            int(r.get("timeframe") or r.get("tf") or r.get("tf_min") or 0),
            frozenset((r.get("params") or {}).items())
        )
        buckets.setdefault(key, []).append(r)

    out: Dict[Tuple, Dict[str, Any]] = {}
    for key, grp in buckets.items():
        wts = [float(x.get("n_trades", 0) or 0.0) for x in grp]
        if sum(wts) == 0:
            wts = [1.0] * len(grp)

        def vlist(k: str, default=0.0) -> List[float]:
            return [float(x.get(k, default) or default) for x in grp]

        out[key] = {
            "symbol": key[0],
            "strategy": key[1],
            "timeframe": key[2],
            "params": dict(key[3]),
            "score": _wavg(vlist("score"), wts, 0.0),
            "pnl_total": _wavg(vlist("pnl_total"), wts, 0.0),
            "R": _wavg(vlist("R"), wts, 0.0),
            "sharpe": _wavg(vlist("sharpe"), wts, 0.0),
            "win_rate": _wavg(vlist("win_rate"), wts, 0.0),
            "max_dd": max((abs(v) for v in vlist("max_dd", 0.0)), default=0.0) if grp else 0.0,
            "n_trades": int(sum(float(x.get("n_trades", 0) or 0.0) for x in grp)),
            "variant": next((x.get("variant") for x in grp if "variant" in x), None),
        }
    return out

def select_topk_per_symbol_tf(rows: List[Dict[str, Any]], k: int = 3) -> Dict[Tuple[str, int], List[Dict[str, Any]]]:
    """Pick top-k across strategies/params per (symbol, timeframe)."""
    buckets: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for r in rows:
        key = (r.get("symbol"), int(r.get("timeframe") or r.get("tf") or r.get("tf_min") or 0))
        buckets.setdefault(key, []).append(r)
    out: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for key, grp in buckets.items():
        grp.sort(key=lambda x: x.get("score", float("-inf")), reverse=True)
        out[key] = grp[:k]
    return out

def select_topk_per_symbol_tf_strategy(rows: List[Dict[str, Any]], k: int = 1) -> Dict[Tuple[str, int, str], List[Dict[str, Any]]]:
    """Pick top-k per strategy (best params) per (symbol, timeframe)."""
    buckets: Dict[Tuple[str, int, str], List[Dict[str, Any]]] = {}
    for r in rows:
        key = (r.get("symbol"), int(r.get("timeframe") or r.get("tf") or r.get("tf_min") or 0), r.get("strategy"))
        buckets.setdefault(key, []).append(r)
    out: Dict[Tuple[str, int, str], List[Dict[str, Any]]] = {}
    for key, grp in buckets.items():
        grp.sort(key=lambda x: x.get("score", float("-inf")), reverse=True)
        out[key] = grp[:k]
    return out

# ---------------- Next-day eligibility (per row; no ranking needed) ----------------

def decide_next_day(rows: List[Dict[str, Any]], policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Add per-row eligibility flags; no cross-mixing."""
    if not rows:
        return []

    defaults = dict(
        require_positive_return=True,
        min_trades=20,
        min_R=0.0,
        min_pnl_total=None,
        min_sharpe=0.25,
        min_win_rate=0.50,
        max_drawdown=None,
        max_turnover=None,
        max_avg_hold_min=None,
        max_latency_ms=None,
        recency_days=None,
    )
    P = {**defaults, **(policy or {})}

    def _recent_ok(r: Dict[str, Any]) -> bool:
        if not P.get("recency_days"):
            return True
        ts = _parse_dt(r.get("asof") or r.get("end_date") or r.get("last_ts") or r.get("last_bar"))
        if not ts:
            return False
        age_days = (datetime.utcnow() - ts).total_seconds() / 86400.0
        return age_days <= float(P["recency_days"])

    for r in rows:
        reasons: List[str] = []
        if P.get("require_positive_return", True):
            eff = _effective_return_val(r)
            if eff is None or eff <= 0.0:
                r["eligible"] = False
                r["eligibility_reasons"] = ["non_positive_return"]
                continue
        if not _recent_ok(r):
            reasons.append("stale_metrics")
        if int(r.get("n_trades", 0) or 0) < int(P["min_trades"]):
            reasons.append("min_trades")
        if P["min_R"] is not None and (r.get("R") is not None):
            try:
                if float(r["R"]) < float(P["min_R"]):
                    reasons.append("min_R")
            except Exception:
                reasons.append("bad_R")
        if P["min_pnl_total"] is not None and (r.get("pnl_total") is not None):
            try:
                if float(r["pnl_total"]) < float(P["min_pnl_total"]):
                    reasons.append("min_pnl_total")
            except Exception:
                reasons.append("bad_pnl_total")
        if r.get("sharpe") is None or float(r.get("sharpe", 0.0) or 0.0) < float(P["min_sharpe"]):
            reasons.append("min_sharpe")
        wr = r.get("win_rate")
        if wr is None:
            reasons.append("missing_win_rate")
        else:
            wrf = float(wr)
            if wrf > 1.0:  # was in %
                wrf /= 100.0
            if wrf < float(P["min_win_rate"]):
                reasons.append("min_win_rate")
        if P["max_drawdown"] is not None:
            dd_mag = abs(float(r.get("max_dd", 0.0) or 0.0))
            if dd_mag > float(P["max_drawdown"]):
                reasons.append("max_drawdown")
        if P["max_turnover"] is not None and (r.get("turnover") is not None):
            try:
                if float(r["turnover"]) > float(P["max_turnover"]):
                    reasons.append("max_turnover")
            except Exception:
                reasons.append("bad_turnover")
        if P["max_avg_hold_min"] is not None and (r.get("avg_hold_min") is not None):
            try:
                if float(r["avg_hold_min"]) > float(P["max_avg_hold_min"]):
                    reasons.append("max_avg_hold_min")
            except Exception:
                reasons.append("bad_avg_hold_min")
        if P["max_latency_ms"] is not None and (r.get("latency_ms") is not None):
            try:
                if float(r["latency_ms"]) > float(P["max_latency_ms"]):
                    reasons.append("max_latency_ms")
            except Exception:
                reasons.append("bad_latency_ms")

        r["eligible"] = len(reasons) == 0
        r["eligibility_reasons"] = reasons
    return rows

def eligible_by_symbol_tf(rows: List[Dict[str, Any]], policy: Dict[str, Any], limit_per_bucket: Optional[int] = None) -> Dict[Tuple[str, int], List[Dict[str, Any]]]:
    """Filter by `decide_next_day` and group by (symbol, timeframe)."""
    decide_next_day(rows, policy)
    out: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for r in rows:
        sym = r.get("symbol")
        tf = int(r.get("timeframe") or r.get("tf") or r.get("tf_min") or 0)
        if not r.get("eligible"):
            continue
        out.setdefault((sym, tf), []).append(r)
    if limit_per_bucket is not None:
        for k, lst in out.items():
            lst.sort(key=lambda x: x.get("score", 0.0), reverse=True)
            out[k] = lst[: int(limit_per_bucket)]
    return out

def export_next_day_per_symbol_tf(eligible_map: Dict[Tuple[str, int], List[Dict[str, Any]]], out_path: str) -> None:
    """Legacy helper: exports selections by (symbol×tf) with raw metrics."""
    import os, yaml as _yaml
    doc = {"selections": []}
    for (sym, tf), rows in sorted(eligible_map.items()):
        entries = []
        for r in rows:
            entries.append({
                "strategy": r.get("strategy"),
                "params": r.get("params") or {},
                "variant": r.get("variant"),
                "score": float(r.get("score", 0.0)),
                "R": float(r.get("R", 0.0) or 0.0),
                "pnl_total": float(r.get("pnl_total", 0.0) or 0.0),
                "sharpe": float(r.get("sharpe", 0.0) or 0.0),
                "win_rate": float(r.get("win_rate", 0.0) or 0.0),
                "max_dd": float(r.get("max_dd", 0.0) or 0.0),
            })
        doc["selections"].append({"symbol": sym, "tf_min": int(tf), "entries": entries})
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as f:
        _yaml.safe_dump(doc, f, sort_keys=False)
