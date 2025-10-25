# analytics/pairs/find_pairs.py
# Scans 120d golden bars from Postgres for 3m/5m/15m intraday cointegrated pairs
# and writes configs/pairs.yaml with the strongest candidates per timeframe.
import asyncio
import itertools
import os
from concurrent.futures import ProcessPoolExecutor

import asyncpg
import numpy as np
import yaml
from datetime import datetime, timedelta, timezone
from scipy.stats import pearsonr
from statsmodels.tsa.stattools import coint
from tqdm import tqdm

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")
PAIRS_YAML = "configs/pairs.yaml"

TF_CONFIG = {
    "3m": {"table": "bars_3m_golden"},
    "5m": {"table": "bars_5m_golden"},
    "15m": {"table": "bars_15m_golden"},
}
SCAN_TFS = ["3m", "5m", "15m"]

SYMBOL_MIN_ROWS = {"3m": 2200, "5m": 1600, "15m": 900}
CANDIDATE_LIMIT = 120
MIN_OVERLAP = 220
MIN_CORR = 0.72
MAX_COIN_PVAL = 0.025
BETA_WINDOW = 800
LOOKBACK_BARS = 120
BETA_LOOKBACK = 300
ENTRY_Z = 1.5
EXIT_Z = 0.75
STOP_Z = 3.0
MAX_HOLD_MIN = 500
NOTIONAL_PER_LEG = 100_000
ALLOW_SHORT = True
HISTORY_LOOKBACK_DAYS = 120
CPU_COUNT = os.cpu_count() or 2
ENV_WORKERS = os.getenv("PAIRS_PROCESS_WORKERS")
if ENV_WORKERS:
    try:
        PROCESS_WORKERS = max(1, int(ENV_WORKERS))
    except ValueError:
        PROCESS_WORKERS = CPU_COUNT
else:
    PROCESS_WORKERS = CPU_COUNT
POOL_CHUNKSIZE = 16


def load_yaml_default_tf() -> str:
    try:
        with open(PAIRS_YAML, "r") as f:
            cfg = yaml.safe_load(f) or {}
        candidate = (cfg.get("defaults") or {}).get("default_tf")
        if candidate in TF_CONFIG:
            return candidate
    except FileNotFoundError:
        pass
    return SCAN_TFS[0]


async def load_pool():
    return await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS
    )


async def fetch_close_series(pool, symbol, start_utc, tf_key):
    table = TF_CONFIG[tf_key]["table"]
    sql = f"""
        SELECT ts, c
        FROM {table}
        WHERE symbol = $1 AND ts >= $2
        ORDER BY ts
    """
    async with pool.acquire() as con:
        rows = await con.fetch(sql, symbol, start_utc)
    if not rows:
        return None, None
    ts = np.array([int(r["ts"].timestamp()) for r in rows], dtype=np.int64)
    px = np.array([float(r["c"]) for r in rows], dtype=np.float64)
    return ts, px


async def load_symbols(pool, tf_key, start_utc):
    table = TF_CONFIG[tf_key]["table"]
    min_rows = SYMBOL_MIN_ROWS[tf_key]
    sql = f"""
        SELECT symbol, count(*) AS n
        FROM {table}
        WHERE ts >= $1
        GROUP BY 1
        HAVING count(*) >= $2
        ORDER BY n DESC
        LIMIT $3
    """
    async with pool.acquire() as con:
        rows = await con.fetch(sql, start_utc, min_rows, CANDIDATE_LIMIT)
    return [r["symbol"] for r in rows]


def align(ts1, px1, ts2, px2):
    index1 = {t: i for i, t in enumerate(ts1)}
    index2 = {t: i for i, t in enumerate(ts2)}
    common = sorted(set(index1.keys()) & set(index2.keys()))
    if len(common) < MIN_OVERLAP:
        return None, None, None
    x = np.array([px1[index1[t]] for t in common], dtype=np.float64)
    y = np.array([px2[index2[t]] for t in common], dtype=np.float64)
    return np.array(common, dtype=np.int64), x, y


def ols_beta(x, y):
    x1 = np.vstack([np.ones_like(x), x]).T
    beta = np.linalg.lstsq(x1, y, rcond=None)[0][1]
    return float(beta)

_PAIR_DATA = {}


def _init_process_pool(serialized_data):
    global _PAIR_DATA
    _PAIR_DATA = serialized_data


def _evaluate_pair_core(leg_x, data_x, leg_y, data_y):
    ts, x, y = align(*data_x, *data_y)
    if ts is None:
        return None
    corr, _ = pearsonr(x, y)
    if abs(corr) < MIN_CORR:
        return None
    try:
        pval = float(coint(y, x, trend="c")[1])
    except (ValueError, np.linalg.LinAlgError):
        return None
    if pval > MAX_COIN_PVAL:
        return None
    beta = ols_beta(x[-BETA_WINDOW:], y[-BETA_WINDOW:]) if len(x) > BETA_WINDOW else ols_beta(x, y)
    return {
        "leg_x": leg_x,
        "leg_y": leg_y,
        "corr": float(corr),
        "pval": pval,
        "beta": float(beta),
    }


def evaluate_pair(args):
    leg_x, data_x, leg_y, data_y = args
    return _evaluate_pair_core(leg_x, data_x, leg_y, data_y)


def evaluate_pair_process(combo):
    leg_x, leg_y = combo
    data_x = _PAIR_DATA.get(leg_x)
    data_y = _PAIR_DATA.get(leg_y)
    if data_x is None or data_y is None:
        return None
    return _evaluate_pair_core(leg_x, data_x, leg_y, data_y)


async def scan_timeframe(pool, tf_key, start_utc):
    print(f"[pairs-scan] tf={tf_key} loading symbols…")
    symbols = await load_symbols(pool, tf_key, start_utc)
    if not symbols:
        print(f"[pairs-scan] tf={tf_key} found no symbols with sufficient history")
        return []
    print(f"[pairs-scan] tf={tf_key} loaded {len(symbols)} candidate symbols")

    tasks = [fetch_close_series(pool, sym, start_utc, tf_key) for sym in symbols]
    print(f"[pairs-scan] tf={tf_key} fetching bar series for {len(tasks)} symbols")
    series = await asyncio.gather(*tasks)

    data = {
        sym: (ts, px)
        for sym, (ts, px) in zip(symbols, series)
        if ts is not None and len(ts) >= MIN_OVERLAP
    }
    if len(data) < 2:
        print(f"[pairs-scan] tf={tf_key} insufficient aligned history after filtering")
        return []

    pair_total = len(data) * (len(data) - 1) // 2
    print(
        f"[pairs-scan] tf={tf_key} evaluating {pair_total} symbol combinations "
        f"with {PROCESS_WORKERS} workers"
    )
    combos = itertools.combinations(data.keys(), 2)
    serialized_data = {sym: payload for sym, payload in data.items()}
    candidates = []
    with ProcessPoolExecutor(
        max_workers=PROCESS_WORKERS,
        initializer=_init_process_pool,
        initargs=(serialized_data,),
    ) as executor:
        for result in tqdm(
            executor.map(evaluate_pair_process, combos, chunksize=POOL_CHUNKSIZE),
            total=pair_total,
            desc=f"{tf_key} pairs",
            leave=False,
        ):
            if result is not None:
                candidates.append(result)

    candidates.sort(key=lambda item: (item["pval"], -abs(item["corr"])))
    return candidates


async def main():
    start_utc = datetime.now(timezone.utc) - timedelta(days=HISTORY_LOOKBACK_DAYS)
    default_tf = load_yaml_default_tf()
    pool = await load_pool()

    scoring_cfg = {
        "risk_per_trade": 0.01,
        "topk": 40,
        "per_symbol": 2,
        "budgets": {"LOW": 0.4, "MED": 0.35, "HIGH": 0.25},
        "reserve_frac": 0.20,
        "dd_buckets": {"LOW": 50_000.0, "MED": 100_000.0, "HIGH": 150_000.0},
        "next_day_path": "configs/pairs_next_day.yaml",
    }

    raw_pairs = []
    counts = {}
    try:
        for tf_key in SCAN_TFS:
            selected = await scan_timeframe(pool, tf_key, start_utc)
            counts[tf_key] = len(selected)
            for item in selected:
                enriched = item.copy()
                enriched["tf"] = tf_key
                raw_pairs.append(enriched)
    finally:
        await pool.close()

    raw_pairs.sort(key=lambda itm: (itm["tf"], itm["pval"], -abs(itm["corr"])))

    yaml_pairs = [
        {
            "leg_x": item["leg_x"],
            "leg_y": item["leg_y"],
            "tf": item["tf"],
            "lookback": LOOKBACK_BARS,
        }
        for item in raw_pairs
    ]

    defaults = {
        "kafka_broker": "localhost:9092",
        "in_topic": "bars.1m",
        "out_topic": "orders",
        "group_id": "pairs_zmeanrev_runner",
        "metrics_port": 8017,
        "ist_flatten_hhmm": "15:15",
        "default_tf": default_tf,
    }
    output = {"pairs": yaml_pairs, "defaults": defaults, "scoring": scoring_cfg}
    os.makedirs(os.path.dirname(PAIRS_YAML), exist_ok=True)
    with open(PAIRS_YAML, "w") as f:
        yaml.safe_dump(output, f, sort_keys=False)

    for tf_key, count in counts.items():
        print(f"[pairs-scan] tf={tf_key} selected {count} pairs")
    print(f"[pairs-scan] wrote {PAIRS_YAML} with {len(yaml_pairs)} pairs (duplicates retained)")


if __name__ == "__main__":
    asyncio.run(main())
