# backtest/persistence.py
from __future__ import annotations
import os
import json
import math
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime

# ---------------- Parquet helpers ----------------
def _load_parquet(path: str) -> List[Dict[str, Any]]:
    try:
        import pandas as pd  # uses pyarrow/fastparquet if available
        if not os.path.exists(path):
            return []
        df = pd.read_parquet(path)
        rows: List[Dict[str, Any]] = df.to_dict(orient="records")
        for r in rows:
            p = r.get("params")
            if isinstance(p, str):
                try:
                    r["params"] = json.loads(p)
                except Exception:
                    r["params"] = {}
            elif p is None:
                r["params"] = {}
            if "timeframe" not in r:
                if "tf_min" in r:
                    r["timeframe"] = int(r["tf_min"])
                elif "tf" in r:
                    r["timeframe"] = int(r["tf"])
            if "symbol" in r and r["symbol"] is not None:
                r["symbol"] = str(r["symbol"])
            if "strategy" in r and r["strategy"] is not None:
                r["strategy"] = str(r["strategy"])
        return rows
    except Exception as e:
        print(f"[persistence] parquet read failed for {path}: {e}")
        return []

# Public shim used by scheduler (added)
def load_sidecar_rows(run_id: int, kind: str = "summary") -> List[Dict[str, Any]]:
    """
    Load a sidecar Parquet: runs/run_<id>_<kind>.parquet (kind defaults to 'summary').
    Returns [] if missing.
    """
    path = os.path.join("runs", f"run_{run_id}_{kind}.parquet")
    rows = _load_parquet(path)
    if rows:
        print(f"[persistence] loaded {len(rows)} rows from sidecar: {path}")
    else:
        print(f"[persistence] sidecar not found: {path}")
    return rows

# ---------------- Postgres fallback (optional) ----------------
_PG_ENV = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    db=os.getenv("POSTGRES_DB", "trading"),
    user=os.getenv("POSTGRES_USER", "trader"),
    password=os.getenv("POSTGRES_PASSWORD", "trader"),
)

def _pg_connect():
    try:
        import psycopg
        conn = psycopg.connect(
            host=_PG_ENV["host"],
            port=_PG_ENV["port"],
            dbname=_PG_ENV["db"],
            user=_PG_ENV["user"],
            password=_PG_ENV["password"],
            autocommit=True,
        )
        return conn
    except Exception as e:
        # print(f"[persistence] psycopg not available or failed to connect: {e}")
        return None

def get_latest_run_id() -> Optional[int]:
    run_ids = set()
    try:
        for fname in os.listdir("runs"):
            if not fname.startswith("run_"):
                continue
            if not (fname.endswith("_summary.parquet") or fname.endswith("_raw.parquet")):
                continue
            parts = fname.split("_")
            # expect: run_<id>_summary.parquet or run_<id>_raw.parquet
            if len(parts) >= 3:
                try:
                    rid = int(parts[1])
                    run_ids.add(rid)
                except Exception:
                    pass
    except FileNotFoundError:
        pass

    if run_ids:
        rid = max(run_ids)
        print(f"[persistence] latest run_id (sidecar) = {rid}")
        return rid

    conn = _pg_connect()
    if not conn:
        print("[persistence] no sidecars and no DB; latest run_id = None")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT max(run_id) FROM backtest_runs")
            row = cur.fetchone()
            rid = int(row[0]) if row and row[0] is not None else None
            print(f"[persistence] latest run_id (DB) = {rid}")
            return rid
    except Exception as e:
        print(f"[persistence] get_latest_run_id DB error: {e}")
        return None
    finally:
        conn.close()

def _fetch_from_db(run_id: int) -> List[Dict[str, Any]]:
    conn = _pg_connect()
    if not conn:
        return []
    try:
        sql = """
            SELECT symbol, strat AS strategy, variant,
                   params::text, tf_min AS timeframe,
                   trades AS n_trades, winrate AS win_rate,
                   pnl AS pnl_total
            FROM backtest_summary
            WHERE run_id = %s
        """
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            rows = []
            for (symbol, strategy, variant, params_txt, timeframe, ntr, wr, pnl) in cur.fetchall():
                try:
                    params = json.loads(params_txt) if params_txt else {}
                except Exception:
                    params = {}
                rows.append(dict(
                    symbol=symbol, strategy=strategy, variant=variant,
                    params=params, timeframe=int(timeframe),
                    n_trades=int(ntr or 0), win_rate=float(wr or 0.0),
                    pnl_total=float(pnl or 0.0),
                    R=None, sharpe=None, sortino=None, max_dd=None, ulcer=None,
                    turnover=None, avg_hold_min=None, stability=None,
                ))
            return rows
    except Exception as e:
        print(f"[persistence] fetch_from_db error: {e}")
        return []
    finally:
        conn.close()

# ---------------- Public: fetch results ----------------
def fetch_results(run_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Load summary rows for the given run_ids.
    Prefers sidecar Parquet: runs/run_<id>_summary.parquet
    Falls back to DB table backtest_summary if parquet missing.
    """
    if not run_ids:
        rid = get_latest_run_id()
        run_ids = [rid] if rid is not None else []

    out: List[Dict[str, Any]] = []
    for rid in run_ids:
        if rid is None:
            continue
        sidecar = os.path.join("runs", f"run_{rid}_raw.parquet")
        rows = _load_parquet(sidecar)
        if rows:
            print(f"[persistence] loaded {len(rows)} rows from sidecar: {sidecar}")
            out.extend(rows)
        else:
            print(f"[persistence] sidecar missing for run {rid}; trying DB…")
            out.extend(_fetch_from_db(rid))
    return out

# ---------------- Fine grid expansion ----------------
def _around(v: float, span: float, lo: float | None = None, hi: float | None = None) -> List[float]:
    a = max(v * (1.0 - span), lo if lo is not None else -float("inf"))
    b = min(v * (1.0 + span), hi if hi is not None else  float("inf"))
    cand = sorted({v, a, b})
    return [round(x, 6) for x in cand]

def _int_around(v: int, span: float, lo: int | None = None, hi: int | None = None) -> List[int]:
    a = int(math.floor(v * (1.0 - span)))
    b = int(math.ceil (v * (1.0 + span)))
    if lo is not None: a = max(a, lo)
    if hi is not None: b = min(b, hi)
    cand = sorted({v, a, b})
    return [int(x) for x in cand if x > 0]

def _param_span(name: str, base: Any, frac: float) -> List[Any]:
    if isinstance(base, bool):
        return [base]
    if isinstance(base, int):
        return _int_around(base, frac, 1, None)
    if isinstance(base, float):
        return _around(base, frac, 0.0, None)
    return [base]

def expand_fine_grid(coarse_cfg: Dict[str, Any],
                     winners: List[Dict[str, Any]],
                     radius_frac: float = 0.25) -> Dict[str, Any]:
    strat_map = {s["name"]: s for s in coarse_cfg.get("strategies", [])}
    fine = {"strategies": []}
    by_stf: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for w in winners:
        st = w.get("strategy"); tf = int(w.get("timeframe"))
        by_stf.setdefault((st, tf), []).append(w)

    for sname, seed in strat_map.items():
        entry = {
            "name": sname,
            "module": seed["module"],
            "class": seed["class"],
            "params_by_tf": {},
        }
        if "constraints" in seed:
            entry["constraints"] = seed["constraints"]

        pbtf = seed.get("params_by_tf", {})
        all_tfs = sorted(int(k) for k in pbtf.keys())
        for tf in all_tfs:
            tf_key = str(tf)
            coarse_grid = pbtf.get(tf_key, {}) or {}
            winners_here = by_stf.get((sname, tf), [])
            if not winners_here:
                entry["params_by_tf"][tf_key] = coarse_grid
                continue

            accum: Dict[str, set] = {k: set() for k in coarse_grid.keys()}
            for k, v in coarse_grid.items():
                if isinstance(v, list):
                    for x in v:
                        accum[k].add(x)
                else:
                    accum[k].add(v)

            for w in winners_here:
                params = (w.get("params") or {}).copy()
                for k in coarse_grid.keys():
                    if k not in params:
                        continue
                    v = params[k]
                    for cand in _param_span(k, v, radius_frac):
                        accum[k].add(cand)

            refined: Dict[str, List[Any]] = {}
            for k, vals in accum.items():
                vv = list(vals)
                try:
                    if all(isinstance(x, int) or (isinstance(x, float) and float(x).is_integer()) for x in vv):
                        vv = sorted(set(int(round(float(x))) for x in vv))
                    elif all(isinstance(x, (int, float)) for x in vv):
                        vv = sorted(float(x) for x in vv)
                    else:
                        vv = sorted(vv, key=lambda x: str(x))
                except Exception:
                    vv = sorted(list(vals), key=lambda x: str(x))
                refined[k] = vv
            entry["params_by_tf"][tf_key] = refined

        fine["strategies"].append(entry)
    return fine

# ---------------- YAML writer for next_day ----------------
def _prune_nulls(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}

def write_next_day_yaml(out_path: str,
                        picks: List[Dict[str, Any]],
                        budget_buckets: Dict[str, float] | None = None,
                        reserve_frac: float | None = None) -> None:
    import yaml as _yaml
    doc: Dict[str, Any] = {"selections": []}
    for r in picks:
        params = _prune_nulls(r.get("params") or {})
        stats = r.get("stats") or {}
        pnl = float(stats.get("pnl", 0.0) or 0.0)
        ntr = int(stats.get("n_trades", 0) or 0)
        wr  = float(stats.get("win_rate", 0.0) or 0.0)
        ppt = float(stats.get("profit_per_trade", (pnl / ntr) if ntr > 0 else 0.0))
        hold = float(stats.get("avg_hold_min", 0.0) or 0.0)
        notional = r.get("notional")
        base_notional = r.get("base_notional")
        notional_per_leg = r.get("notional_per_leg")
        leverage = r.get("leverage")
        row = {
            "symbol": str(r.get("symbol")),
            "strategy": str(r.get("strategy")),
            "timeframe": int(r.get("timeframe")),
            "params": params,
            "max_dd": float(r.get("max_dd", 0.0) or 0.0),
            "score": float(r.get("score", 0.0) or 0.0),
            "bucket": r.get("bucket", "MED"),
            "risk_per_trade": float(r.get("risk_per_trade", 0.01)),
            "stats": {
                "pnl": round(pnl, 4),
                "n_trades": int(ntr),
                "win_rate": round(wr, 4),
                "profit_per_trade": round(ppt, 4),
                "avg_hold_min": round(hold, 4),
            }
        }
        if notional is not None:
            try:
                row["notional"] = float(notional)
            except Exception:
                pass
        if base_notional is not None:
            try:
                row["base_notional"] = float(base_notional)
            except Exception:
                pass
        if notional_per_leg is not None:
            try:
                row["notional_per_leg"] = float(notional_per_leg)
            except Exception:
                pass
        if leverage is not None:
            try:
                row["leverage"] = float(leverage)
            except Exception:
                pass
        doc["selections"].append(row)

    if budget_buckets:
        doc["budgets"] = {k: float(v) for k, v in budget_buckets.items()}
    if reserve_frac is not None:
        doc["reserve_frac"] = float(reserve_frac)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as f:
        _yaml.safe_dump(doc, f, sort_keys=False)
    print(f"[persistence] wrote next_day → {out_path} (selections={len(doc['selections'])})")

# ---------------- Compatibility shims (for grid_runner / scheduler) ----------------

# in backtest/persistence.py

def save_results(run_id: int, results: list[dict], kind: str = "raw") -> str:
    """
    Save results to Parquet under runs/run_<id>_<kind>.parquet.
    - params -> JSON string to avoid empty-struct Parquet issue
    - stats.* flattened into columns
    """
    from pathlib import Path
    import pandas as pd
    import numpy as np
    import json as _json

    def _to_py(x):
        if isinstance(x, (np.generic,)):
            return x.item()
        return x

    norm = []
    for r in results:
        rr = dict(r)  # shallow copy
        # flatten stats
        stats = rr.pop("stats", {}) or {}
        rr["pnl_total"]       = _to_py(stats.get("pnl", 0.0))
        rr["n_trades"]        = _to_py(stats.get("n_trades", 0))
        rr["win_rate"]        = _to_py(stats.get("win_rate", 0.0))
        rr["profit_per_trade"]= _to_py(stats.get("profit_per_trade", 0.0))
        # ensure required scalar fields exist
        rr["symbol"]   = str(rr.get("symbol", ""))
        rr["strategy"] = str(rr.get("strategy", ""))
        rr["timeframe"]= int(rr.get("timeframe", rr.get("tf", 0)) or 0)
        rr.pop("tf", None)
        # params as JSON text (avoid empty struct)
        params = rr.pop("params", {}) or {}
        rr["params_json"] = _json.dumps(params, separators=(",", ":"), ensure_ascii=False)
        norm.append(rr)

    df = pd.DataFrame(norm)
    # Optional: stable column order
    cols = ["symbol","strategy","timeframe","pnl_total","n_trades","win_rate","profit_per_trade","params_json"]
    cols += [c for c in df.columns if c not in cols]
    df = df.reindex(columns=cols)

    outdir = Path("runs")
    outdir.mkdir(parents=True, exist_ok=True)
    path = str(outdir / f"run_{run_id}_{kind}.parquet")
    df.to_parquet(path, index=False)
    print(f"[persistence] wrote {len(df)} rows → {path}")
    return path


def load_run_summary(run_id: int) -> List[Dict[str, Any]]:
    """Load one run's summary parquet."""
    path = os.path.join("runs", f"run_{run_id}_raw.parquet")
    return _load_parquet(path)


def load_run_results(run_id: int) -> List[Dict[str, Any]]:
    """Load one run's raw results parquet."""
    path = os.path.join("runs", f"run_{run_id}_raw.parquet")
    return _load_parquet(path)


# Stub: earlier versions expected this for loading minute bars from DB or parquet
def load_bars_1m_for_symbol(symbol: str) -> List[Dict[str, Any]]:
    """
    Compatibility placeholder.
    In your infra this should load 1m bars for `symbol` (from DB, parquet, or Kafka snapshot).
    Right now returns [] so grid_runner_parallel doesn’t break on import.
    """
    print(f"[persistence] load_bars_1m_for_symbol({symbol}) called → returning []")
    return []
