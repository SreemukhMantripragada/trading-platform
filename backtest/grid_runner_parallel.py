# backtest/grid_runner_parallel.py
"""
Run fine/coarse grid in parallel across symbols with multiprocessing + Postgres bars source.
- Reads symbols from CSV (with subscribe==1) exactly like before.
- Expands per-strategy params from params_by_tf (per timeframe) and applies constraints.
- One job per (symbol, strategy, timeframe, params).
- Uses asyncpg to load bars, multiprocessing Pool to simulate, and persistence.save_results to write sidecar.
"""

from __future__ import annotations
import argparse, asyncio, os, yaml, time
import pandas as pd
import asyncpg
from multiprocessing import Pool, cpu_count
from typing import Dict, Any, List, Iterable
from datetime import datetime, timezone, timedelta

from backtest.persistence import save_results, get_latest_run_id

# ---------------- TF tables ----------------
TF_TABLE = {3: "bars_3m_golden", 5: "bars_5m_golden", 15: "bars_15m_golden"}

def table_for_tf(tf: int) -> str:
    try:
        return TF_TABLE[int(tf)]
    except Exception:
        raise ValueError(f"Unsupported timeframe {tf}; allowed: 3, 5, 15")

# ---------------- Optional helpers (constraints/grid expander) ----------------
try:
    from backtest.param_grid import expand_params_for_tf, apply_constraint
    _HAS_HELPERS = True
except Exception:
    _HAS_HELPERS = False
    import itertools

    def _expand_grid_for_tf(grid: dict) -> list[dict]:
        if not grid:
            return [{}]
        keys = list(grid.keys())
        vals = [v if isinstance(v, list) else [v] for v in grid.values()]
        return [dict(zip(keys, combo)) for combo in itertools.product(*vals)]

    def expand_params_for_tf(strategy_block: dict, tf: int) -> list[dict]:
        pbtf = strategy_block.get("params_by_tf") or {}
        grid = pbtf.get(str(tf)) or {}
        return _expand_grid_for_tf(grid)

    def apply_constraint(combos: list[dict], expr: str) -> list[dict]:
        if not expr:
            return combos
        out = []
        for p in combos:
            try:
                ok = bool(eval(expr, {"__builtins__": {}}, dict(p)))
            except Exception:
                ok = True
            if ok:
                out.append(p)
        return out

# ---------------- DB / bar loading ----------------
PG_HOST=os.getenv("POSTGRES_HOST", "localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT", 5432))
PG_DB=os.getenv("POSTGRES_DB", "trading")
PG_USER=os.getenv("POSTGRES_USER", "trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD", "trader")

async def load_bars_tf(pool, symbol: str, tf: int, days: int) -> pd.DataFrame:
    """
    Load last N days for symbol from the 3m/5m/15m golden tables.
    Uses a concrete timestamptz cutoff to avoid interval encoding issues.
    """
    table = table_for_tf(tf)
    start_ts = datetime.now(timezone.utc) - timedelta(days=int(days))
    sql = f"""
        SELECT ts, o, h, l, c, vol
        FROM {table}
        WHERE symbol = $1
          AND ts >= $2::timestamptz
        ORDER BY ts
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, symbol, start_ts)
    if not rows:
        return pd.DataFrame(columns=["symbol", "tf", "ts", "o", "h", "l", "c", "vol"])
    df = pd.DataFrame([dict(r) for r in rows])
    df.insert(0, "symbol", symbol)
    df.insert(1, "tf", f"{int(tf)}m")
    return df

# ---------------- params expansion ----------------
def _expand_params_for_strategy(block: Dict[str, Any], tfs: Iterable[int]) -> Dict[int, List[Dict[str, Any]]]:
    out: Dict[int, List[Dict[str, Any]]] = {}
    for tf in tfs:
        combos = expand_params_for_tf(block, tf) if _HAS_HELPERS else expand_params_for_tf(block, tf)
        combos = apply_constraint(combos, block.get("constraints", "")) if combos else []
        out[tf] = combos or [{}]
    return out

# ---------------- worker ----------------
def run_one(args: Dict[str, Any]) -> Dict[str, Any]:
    """Run backtest for one symbol+strategy+params set (child process safe)."""
    import backtest.engine as engine
    sym         = args["sym"]
    strat_cls   = args["strat_cls"]
    tf          = args["tf"]
    params      = args["params"]     # dict
    bars        = args["bars"]       # list[dict] (faster to pickle than DF)
    psc         = args["per_stock_capital"]
    exit_policy = args["exit_policy"]
    allow_short = args["allow_short"]
    costs       = args["costs"]

    try:
        return engine.run_backtest(
            sym, strat_cls, tf, params, bars,
            per_stock_capital=psc,
            exit_policy=exit_policy,
            allow_short=allow_short,
            costs=costs,
        )
    except Exception as e:
        return {
            "symbol": sym,
            "strategy": getattr(strat_cls, "__name__", str(strat_cls)),
            "timeframe": tf,
            "params": params,
            "error": str(e),
        }

# ---------------- main ----------------
async def main(cfg_path: str):
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)

    latest = get_latest_run_id() or 0
    run_id = latest + 1
    print(f"[runner] starting run {run_id} from {cfg_path}")

    # ---- symbols from CSV (subscribe==1/true/y/yes) EXACTLY LIKE BEFORE ----
    symbols_csv = cfg["symbols"]["from_tokens_csv"]
    df_syms = pd.read_csv(symbols_csv)
    subcol = df_syms.get("subscribe")
    if subcol is None:
        symbols = df_syms["tradingsymbol"].astype(str).tolist()
    else:
        mask = subcol.astype(str).str.strip().str.lower().isin(["1","true","y","yes"])
        symbols = df_syms.loc[mask, "tradingsymbol"].astype(str).tolist()
    # symbols = symbols[:5]
    strategies: List[Dict[str, Any]] = cfg["strategies"]
    lookback_days = int(cfg.get("days_lookback", cfg.get("lookback_days", 60)))

    per_stock_capital = float(cfg.get("per_stock_capital", 100_000.0))
    allow_short = bool(cfg.get("allow_short", True))

    # exit policy (optional)
    exit_mode = (cfg.get("exit_mode") or "strategy").lower()
    exit_policy = {"mode": exit_mode}
    if exit_mode == "fixed":
        exit_policy["tp_pct"] = float(cfg.get("tp_pct", 0.0))
        exit_policy["sl_pct"] = float(cfg.get("sl_pct", 0.0))
    if cfg.get("squareoff_ist"):
        exit_policy["squareoff_ist"] = str(cfg["squareoff_ist"])

    # costs (optional)
    costs_cfg = cfg.get("costs") or {}
    costs = dict(
        slippage_bps=float(costs_cfg.get("slippage_bps", 0.0)),
        flat_per_side=float(costs_cfg.get("flat_per_side", 0.0)),
        pct_taxes=float(costs_cfg.get("pct_taxes", 0.0)),
    )

    # which TFs to run (only 3/5/15 supported)
    tfs_cfg = cfg.get("timeframes_min") or [3, 5, 15]
    tfs = [tf for tf in map(int, tfs_cfg) if tf in (3, 5, 15)]
    if not tfs:
        raise ValueError("timeframes_min must include at least one of: 3, 5, 15")

    # DB pool
    pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, database=PG_DB
    )

    # preload bars per (symbol, tf)
    bars_map: Dict[tuple, list] = {}
    for sym in symbols:
        for tf in tfs:
            df = await load_bars_tf(pool, sym, tf, lookback_days)
            # engine accepts DF or list-of-dicts; list-of-dicts pickles faster for Pool
            bars_map[(sym, tf)] = df.to_dict(orient="records")
            print(f"[runner] loaded {len(df)} bars for {sym} tf={tf}m")

    await pool.close()

    # prepare work items
    work: List[Dict[str, Any]] = []
    for s in strategies:
        Strat = getattr(__import__(s["module"], fromlist=[s["class"]]), s["class"])
        if "params_by_tf" not in s:
            raise ValueError(f"{s['name']} must define params_by_tf for tf in {tfs}")
        params_by_tf = _expand_params_for_strategy(s, tfs)
        for tf in tfs:
            param_sets = params_by_tf.get(tf, [{}]) or [{}]
            for params in param_sets:
                for sym in symbols:
                    bars_rec = bars_map.get((sym, tf)) or []
                    if not bars_rec:
                        continue
                    work.append({
                        "sym": sym,
                        "strat_cls": Strat,
                        "tf": tf,
                        "params": params or {},
                        "bars": bars_rec,
                        "per_stock_capital": per_stock_capital,
                        "exit_policy": exit_policy,
                        "allow_short": allow_short,
                        "costs": costs,
                    })

    ncores = cpu_count()
    njobs  = len(work)
    print(f"[runner] spawning {njobs} jobs across {ncores} cores")

    t0 = time.time()
    # Use smaller pool if jobs are small; otherwise all cores
    with Pool(processes=min(ncores, max(1, njobs))) as p:
        results = p.map(run_one, work)
    dt = time.time() - t0

    print(f"[runner] finished {len(results)} runs in {dt:.1f}s")

    # save sidecar(s)
    save_results(run_id, results)
    print(f"[runner] results saved under run {run_id}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    asyncio.run(main(args.config))
