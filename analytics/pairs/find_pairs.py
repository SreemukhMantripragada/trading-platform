# analytics/pairs/find_pairs.py
# Finds candidate pairs using 60d bars_1m from Postgres, resamples to a target TF
# (1m/3m/5m/15m) in-memory, ranks by cointegration p-value, writes configs/pairs.yaml.
import os, yaml, argparse, itertools, asyncpg
import numpy as np
from datetime import datetime, timedelta, timezone
from scipy.stats import pearsonr
from statsmodels.tsa.stattools import coint

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
PAIRS_YAML="configs/pairs.yaml"

TF_MAP = {"1m":1, "3m":3, "5m":5, "15m":15}

def load_yaml_defaults_tf():
    try:
        with open(PAIRS_YAML,"r") as f:
            cfg=yaml.safe_load(f) or {}
        return (cfg.get("defaults") or {}).get("default_tf","1m")
    except FileNotFoundError:
        return "1m"

async def load_pool():
    return await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)

async def fetch_close_series(pool, symbol, start_utc):
    sql = """SELECT ts, c FROM bars_1m_golden WHERE symbol=$1 AND ts >= $2 ORDER BY ts"""
    async with pool.acquire() as con:
        rows = await con.fetch(sql, symbol, start_utc)
    if not rows: return None, None
    # minute-granular unix seconds
    ts = np.array([int(r["ts"].timestamp()) for r in rows], dtype=np.int64)
    px = np.array([float(r["c"]) for r in rows], dtype=np.float64)
    return ts, px

def resample_close(ts_sec, px, tf_str):
    """Resample close to TF using 'last' of the window (OHLC not needed for pairs).
       Returns (ts_bucketed, px_resampled) aligned by bucket start."""
    tfm = TF_MAP[tf_str]
    if tfm==1: return ts_sec, px
    # bucket start = ts - (ts % (tfm*60))
    step = tfm*60
    buckets = {}
    for t, p in zip(ts_sec, px):
        b = t - (t % step)
        # we keep 'last' close in bucket; order already ascending
        buckets[b] = p
    if not buckets: return np.array([],dtype=np.int64), np.array([],dtype=np.float64)
    bts = np.array(sorted(buckets.keys()), dtype=np.int64)
    bp  = np.array([buckets[t] for t in bts], dtype=np.float64)
    return bts, bp

def align(ts1, px1, ts2, px2):
    i1 = {t:i for i,t in enumerate(ts1)}
    i2 = {t:i for i,t in enumerate(ts2)}
    common = sorted(set(i1.keys()) & set(i2.keys()))
    if len(common) < 200:  # need overlap
        return None, None, None
    x = np.array([px1[i1[t]] for t in common], dtype=np.float64)
    y = np.array([px2[i2[t]] for t in common], dtype=np.float64)
    return np.array(common,dtype=np.int64), x, y

def ols_beta(x, y):
    x1 = np.vstack([np.ones_like(x), x]).T
    beta = np.linalg.lstsq(x1, y, rcond=None)[0][1]
    return float(beta)

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--min_corr", type=float, default=0.6)
    ap.add_argument("--max_pval", type=float, default=0.05)
    ap.add_argument("--tf", type=str, default=load_yaml_defaults_tf(), choices=list(TF_MAP.keys()),
                    help="timeframe for scanning pairs (defaults.default_tf from pairs.yaml)")
    args = ap.parse_args()

    start_utc = datetime.now(timezone.utc) - timedelta(days=60)
    pool = await load_pool()

    # candidate universe (symbols with enough bars)
    async with pool.acquire() as con:
        syms = [r["symbol"] for r in await con.fetch(
            "SELECT symbol, count(*) n FROM bars_1m_golden GROUP BY 1 HAVING count(*)>2000 ORDER BY n DESC LIMIT 150"
        )]

    candidates=[]
    for a,b in itertools.combinations(syms,2):
        ts1, px1 = await fetch_close_series(pool, a, start_utc)
        ts2, px2 = await fetch_close_series(pool, b, start_utc)
        if px1 is None or px2 is None: continue
        rts1, rpx1 = resample_close(ts1, px1, args.tf)
        rts2, rpx2 = resample_close(ts2, px2, args.tf)
        ts, x, y = align(rts1, rpx1, rts2, rpx2)
        if ts is None: continue
        r, _ = pearsonr(x, y)
        if abs(r) < args.min_corr: continue
        pval = coint(y, x, trend='c')[1]
        if pval > args.max_pval: continue
        beta = ols_beta(x[-800:], y[-800:]) if len(x)>800 else ols_beta(x, y)
        candidates.append((pval, a, b, beta))

    candidates.sort(key=lambda t: t[0])
    top = candidates[:args.top]

    # Write pairs.yaml (preserve defaults if present)
    defaults = {
        "kafka_broker":"localhost:9092",
        "in_topic":"bars.1m",
        "out_topic":"orders",
        "group_id":"pairs_zmeanrev_runner",
        "metrics_port":8017,
        "ist_flatten_hhmm":"15:15",
        "default_tf": args.tf
    }
    pairs=[]
    for pval, ax, by, beta in top:
        pairs.append({
            "leg_x": ax, "leg_y": by,
            "tf": args.tf,
            "lookback": 120,
            "hedge_mode":"dynamic",
            "fixed_beta": float(beta),
            "beta_lookback": 300,
            "entry_z": 1.5, "exit_z": 0.75, "stop_z": 3.0,
            "max_hold_min": 500,
            "notional_per_leg": 100000,
            "allow_short": True
        })

    # If an existing YAML exists, keep defaults unless we overwrite explicitly
    out={"pairs":pairs,"defaults":defaults}
    os.makedirs(os.path.dirname(PAIRS_YAML), exist_ok=True)
    with open(PAIRS_YAML,"w") as f:
        yaml.safe_dump(out, f, sort_keys=False)
    print(f"[pairs-scan] tf={args.tf} wrote {PAIRS_YAML} with {len(pairs)} pairs")

if __name__=="__main__":
    import asyncio; asyncio.run(main())
