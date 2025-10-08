# backtest/coarse_fine_scheduler.py
from __future__ import annotations
import argparse
import subprocess
import sys
import os
import yaml
from typing import Dict, Any, List, Tuple, Optional, Iterable
import json
# Uses your scorer (unchanged)
from backtest.scorer import (
    DEFAULT_GATES,
    DEFAULT_WEIGHTS,
    apply_constraints,
    score_rows,
    aggregate_by_config,
)

# File persistence helpers (sidecar I/O)
from backtest.persistence import (
    load_sidecar_rows,     # (run_id: str|int) -> List[dict]
    get_latest_run_id
)

# ---------------------------
# CLI helpers
# ---------------------------

PY_EXE = sys.executable

def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

def _dump_yaml(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(obj, f, sort_keys=False)

def _launch_grid(config_path: str, comment: str) -> str:
    """
    Run the parallel grid and parse the run_id printed by the runner.
    Falls back to scanning the runner's final 'runs/run_<id>_summary.parquet' hint if needed.
    """
    print(f"[scheduler] launching grid_runner_parallel with {config_path}")
    proc = subprocess.run(
        [PY_EXE, "-m", "backtest.grid_runner_parallel", "--config", config_path],
        check=True, capture_output=True, text=True
    )
    # stdout = proc.stdout
    # print(stdout)
    run_id: Optional[str] = get_latest_run_id()
    return run_id

# ---------------------------
# Audit & selection utilities
# ---------------------------

def audit_presence(rows: List[Dict[str, Any]]) -> None:
    keys = [
        "R","pnl_total","sharpe","sortino","win_rate","max_dd","ulcer",
        "turnover","avg_hold_min","stability","n_trades"
    ]
    counts = {k: 0 for k in keys}
    for r in rows:
        for k in keys:
            if r.get(k) is not None:
                counts[k] += 1
    print(f"[audit] raw: n={len(rows)} | " + ", ".join([f"{k}={counts[k]}" for k in keys]))

def _row_key(r: dict) -> tuple:
    """Return a hashable key for a result row for comparison."""
    return (
        r.get("symbol"),
        r.get("strategy"),
        int(r.get("timeframe", 0)),
        json.dumps(r.get("params") or {}, sort_keys=True),
    )
def _top_rejection_counts(filtered: list[dict], original: list[dict]) -> list[tuple[str,int]]:
    """Find most common rejection reasons among rows removed by filters."""
    filt_keys = {_row_key(r) for r in filtered}
    rejected = [r for r in original if _row_key(r) not in filt_keys]
    reasons = [r.get("reject_reason") or r.get("error") or "unknown" for r in rejected]
    from collections import Counter
    return Counter(reasons).most_common()

def select_top_configs(
    rows: List[Dict[str, Any]],
    gates: Dict[str, Any],
    weights: Dict[str, float],
    topk_per_strategy: int
) -> List[Dict[str, Any]]:
    """
    1) Hard constraints
    2) Score within cohorts (scorer.py)
    3) Aggregate across symbols to per-(strategy, timeframe, params) to avoid symbol leakage in the *selection* list
       (We still keep symbol-specific info for final next_day in fine stage.)
    4) Take topK per (strategy, timeframe)
    """
    if not rows:
        return []

    filtered = apply_constraints(rows, gates)
    print("[audit] top rejection reasons:", _top_rejection_counts(filtered, rows)[:10])

    if not filtered:
        return []

    scored = score_rows(filtered, weights)
    cfg_map = aggregate_by_config(scored)  # keyed by (strategy, timeframe, frozenset(params))
    ranked = sorted(cfg_map.values(), key=lambda x: x["score"], reverse=True)

    out: List[Dict[str, Any]] = []
    used: Dict[Tuple[str,int], int] = {}
    for r in ranked:
        key = (r["strategy"], int(r["timeframe"]))
        if used.get(key, 0) >= int(topk_per_strategy):
            continue
        used[key] = used.get(key, 0) + 1
        out.append(r)
    return out

# ---------------------------
# Fine grid builder (keeps structure identical to coarse)
# ---------------------------

def _neighbor_values(val: float, grid: List[float]) -> List[float]:
    """
    Given a chosen value and the original candidate grid for that tf/param,
    pick a small band of neighbors around it (±1 step on each side when available).
    """
    if not grid:
        return []
    # ensure unique sorted floats
    xs = sorted({float(x) for x in grid})
    try:
        i = xs.index(float(val))
    except ValueError:
        # If winner lies outside or not exactly in grid, snap to closest
        i = min(range(len(xs)), key=lambda k: abs(xs[k]-float(val)))
    lo = max(0, i-1)
    hi = min(len(xs)-1, i+1)
    return xs[lo:hi+1]

def _merge_same_structure_as_coarse(coarse_cfg: Dict[str, Any], narrowed: Dict[str, Dict[str, Dict[str, List[float]]]]) -> Dict[str, Any]:
    """
    Build a fine config with the **same YAML structure** as the coarse:
      - top-level keys copied through (symbols, timeframes_min, processes, costs, etc.)
      - for each strategy: name/module/class/constraints kept identical
      - params_by_tf updated to narrowed lists (only for TFs present in coarse).
    """
    out = {k: v for k, v in coarse_cfg.items() if k != "strategies"}
    fine_strats: List[Dict[str, Any]] = []
    for s in coarse_cfg.get("strategies", []):
        name = s["name"]
        s_copy = dict(s)
        pbt: Dict[str, Any] = {}
        coarse_pbt = s.get("params_by_tf", {}) or {}
        narrowed_for_s = narrowed.get(name, {})
        for tf_key, coarse_params in coarse_pbt.items():
            tf_narrow = narrowed_for_s.get(str(tf_key), {})
            # If we didn't narrow anything (e.g., no winners for this strategy/tf), keep coarse to allow exploration
            pbt[str(tf_key)] = tf_narrow if tf_narrow else coarse_params
        s_copy["params_by_tf"] = pbt
        fine_strats.append(s_copy)
    out["strategies"] = fine_strats
    return out

def build_fine_grid(
    coarse_cfg: Dict[str, Any],
    winners: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    winners are aggregated entries from select_top_configs (per strategy×tf×params).
    For each winner, we narrow each parameter at that TF to ±1 neighbors (where available) using the original coarse grid.
    """
    # index original coarse grids: {strategy_name: {tf_str: {param: [values...]}}}
    coarse_index: Dict[str, Dict[str, Dict[str, List[float]]]] = {}
    for s in coarse_cfg.get("strategies", []):
        name = s["name"]
        pbt = s.get("params_by_tf", {}) or {}
        coarse_index[name] = {}
        for tf_key, pmap in pbt.items():
            coarse_index[name][str(tf_key)] = {k: [float(x) for x in (v if isinstance(v, list) else [v])]
                                               for k, v in (pmap or {}).items()}

    # Build narrowed lists only for params that appear in winners
    narrowed: Dict[str, Dict[str, Dict[str, List[float]]]] = {}
    for w in winners:
        name = w["strategy"]
        tf = str(int(w["timeframe"]))
        params = w.get("params", {}) or {}
        if name not in coarse_index or tf not in coarse_index[name]:
            continue
        src_grid = coarse_index[name][tf]
        dst = narrowed.setdefault(name, {}).setdefault(tf, {})
        for pkey, val in params.items():
            if pkey not in src_grid:
                continue
            neigh = _neighbor_values(float(val), src_grid[pkey])
            if neigh:
                dst[pkey] = neigh

    return _merge_same_structure_as_coarse(coarse_cfg, narrowed)

# ---------------------------
# Next-day YAML writer
# ---------------------------

def _prune_nulls(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}

def _stats_block(r: Dict[str, Any]) -> Dict[str, Any]:
    pnl = float(r.get("pnl_total", 0.0) or 0.0)
    n = int(r.get("n_trades", 0) or 0)
    wr = float(r.get("win_rate", 0.0) or 0.0)
    ppt = (pnl / n) if n > 0 else 0.0
    return dict(pnl=round(pnl, 4), n_trades=n, win_rate=round(wr, 4), profit_per_trade=round(ppt, 4))

def _assign_bucket(dd: float) -> str:
    # Conservative bucketing by magnitude of drawdown
    dd_mag = abs(float(dd or 0.0))
    if dd_mag <= 0.03 * 1e6:   # if your PnL is absolute currency, adapt thresholds; else use % if normalized
        return "LOW"
    if dd_mag <= 0.06 * 1e6:
        return "MED"
    return "HIGH"

def write_next_day_yaml(
    out_path: str,
    picks: List[Dict[str, Any]],
    budget_buckets: Dict[str, float],
    reserve_frac: float = 0.20,
) -> None:
    doc: Dict[str, Any] = {"selections": []}
    for r in picks:
        params = _prune_nulls(r.get("params", {}) or {})
        row = dict(
            symbol=r.get("symbol"),
            strategy=r.get("strategy"),
            timeframe=int(r.get("timeframe")),
            params=params,
            max_dd=float(r.get("max_dd", 0.0) or 0.0),
            score=float(r.get("score", 0.0) or 0.0),
            bucket=_assign_bucket(r.get("max_dd", 0.0)),
            risk_per_trade=0.01,  # can be externalized
            stats=_stats_block(r),
        )
        doc["selections"].append(row)
    doc["budgets"] = dict(LOW=budget_buckets.get("LOW", 0.5),
                          MED=budget_buckets.get("MED", 0.3),
                          HIGH=budget_buckets.get("HIGH", 0.2))
    doc["reserve_frac"] = float(reserve_frac)
    _dump_yaml(out_path, doc)
    print(f"[scheduler] Wrote {out_path} with {len(doc['selections'])} picks.")

# ---------------------------
# Main
# ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coarse", required=True, help="Path to coarse grid yaml")
    ap.add_argument("--fine_out", default="configs/grid_fine.yaml")
    ap.add_argument("--next_day", default="configs/next_day.yaml")
    ap.add_argument("--comment", default="nightly")
    ap.add_argument("--topk", type=int, default=3, help="Top-K per (strategy, timeframe) to seed fine grid")
    ap.add_argument("--per_symbol", type=int, default=5, help="Cap final picks per symbol")
    ap.add_argument("--budget_low", type=float, default=0.5)
    ap.add_argument("--budget_med", type=float, default=0.3)
    ap.add_argument("--budget_high", type=float, default=0.2)
    # Optional override gates/weights yaml for reproducibility
    ap.add_argument("--gates_yaml", default=None)
    ap.add_argument("--weights_yaml", default=None)
    args = ap.parse_args()

    coarse_cfg = _load_yaml(args.coarse)

    # Optional gates/weights overrides
    gates = DEFAULT_GATES.copy()
    weights = DEFAULT_WEIGHTS.copy()
    if args.gates_yaml and os.path.exists(args.gates_yaml):
        gates.update(_load_yaml(args.gates_yaml) or {})
    if args.weights_yaml and os.path.exists(args.weights_yaml):
        weights.update(_load_yaml(args.weights_yaml) or {})

    # -------- COARSE STAGE --------
    run_id_coarse = _launch_grid(args.coarse, f"{args.comment}_coarse")
    rows_coarse = load_sidecar_rows(run_id_coarse, "raw")
    print(f"[scheduler] loaded {len(rows_coarse)} rows from sidecar: runs/run_{run_id_coarse}_raw.parquet")
    audit_presence(rows_coarse)
    # print(rows_coarse[:10])
    winnersA = select_top_configs(rows_coarse, gates, weights, topk_per_strategy=args.topk)
    print(f"[audit] coarse/winners: {len(winnersA)}")
    if not winnersA:
        print("[scheduler] No winners in coarse stage; abort.")
        sys.exit(2)

    # Build FINE grid keeping the exact same structure as COARSE
    fine_cfg = build_fine_grid(coarse_cfg, winnersA)
    _dump_yaml(args.fine_out, fine_cfg)
    print(f"[scheduler] wrote fine grid -> {args.fine_out}")

    # -------- FINE STAGE --------
    run_id_fine = _launch_grid(args.fine_out, f"{args.comment}_fine")
    rows_fine = load_sidecar_rows(run_id_fine, "raw")
    print(f"[scheduler] loaded {len(rows_fine)} rows from sidecar: runs/run_{run_id_fine}_summary.parquet")
    audit_presence(rows_fine)

    # Score *without* cross-symbol mixing for final selection stability:
    filtered_fine = apply_constraints(rows_fine, gates)
    scored_fine = score_rows(filtered_fine, weights)

    # Rank globally by score then greedily cap per symbol
    chosen: List[Dict[str, Any]] = []
    per_sym: Dict[str, int] = {}
    for r in sorted(scored_fine, key=lambda x: x.get("score", float("-inf")), reverse=True):
        sym = r.get("symbol","")
        if (per_sym.get(sym, 0) >= int(args.per_symbol)):
            continue
        if(r["win_rate"]<60):
            continue
        per_sym[sym] = per_sym.get(sym, 0) + 1
        chosen.append(r)
    # Write next_day.yaml with pruned params & stats block
    write_next_day_yaml(
        args.next_day,
        chosen,
        budget_buckets={"LOW": args.budget_low, "MED": args.budget_med, "HIGH": args.budget_high},
        reserve_frac=0.20,
    )

if __name__ == "__main__":
    main()
