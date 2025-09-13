"""
analytics/pairs/find_pairs.py
- Loads symbols from configs/pairs.yaml
- Pulls last N days of bars_1m from Postgres
- For each pair (A,B), computes:
    * corr of returns
    * hedge ratio beta = cov(A,B)/var(B)
    * distance metric = stdev( log(A) - beta*log(B) )
- Keeps top_k by corr (and finite distance), UPSERTs into pairs_universe with beta,
  and writes a stats snapshot in pairs_stats.

Run:
  POSTGRES_* env, then:
  python analytics/pairs/find_pairs.py
"""
from __future__ import annotations
import os, csv, math, yaml, itertools, asyncpg
from statistics import mean
from datetime import date, timedelta

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
CFG_PATH=os.getenv("PAIRS_CFG","configs/pairs.yaml")

def load_cfg():
    return yaml.safe_load(open(CFG_PATH))

def load_symbols(path, limit):
    out=[]
    with open(path, newline="") as f:
        r=csv.DictReader(f)
        for row in r:
            s=(row.get("symbol") or row.get("tradingsymbol") or "").strip()
            if s: out.append(s)
    return out[:limit] if limit and limit>0 else out

async def fetch_close_map(con, symbols, start_d, end_d):
    # returns {symbol: [ (ts,c), ... ]} aligned by time; simple list ok
    res={}
    for sym in symbols:
        rows = await con.fetch(
          "SELECT extract(epoch from ts)::bigint AS ts, c FROM bars_1m WHERE symbol=$1 AND ts::date BETWEEN $2 AND $3 ORDER BY ts",
          sym, start_d, end_d
        )
        res[sym] = [(int(r["ts"]), float(r["c"])) for r in rows]
    return res

def align_series(a, b):
    # align by timestamp for fairness
    ia=ib=0; A=[]; B=[]
    while ia<len(a) and ib<len(b):
        ta,ca=a[ia]; tb,cb=b[ib]
        if ta==tb: A.append(ca); B.append(cb); ia+=1; ib+=1
        elif ta<tb: ia+=1
        else: ib+=1
    return A,B

def corr_beta_and_dist(prices_a, prices_b):
    # returns (corr, beta, dist)
    if len(prices_a)<50 or len(prices_b)<50: return (None,None,None)
    # simple returns to compute corr
    ra=[(prices_a[i]/prices_a[i-1]-1.0) for i in range(1,len(prices_a))]
    rb=[(prices_b[i]/prices_b[i-1]-1.0) for i in range(1,len(prices_b))]
    if len(ra)!=len(rb) or len(ra)<30: return (None,None,None)
    ma,mb=mean(ra),mean(rb)
    varb = sum((x-mb)**2 for x in rb)/(len(rb)-1) or 1e-12
    cov  = sum((ra[i]-ma)*(rb[i]-mb) for i in range(len(ra)))/(len(ra)-1)
    corr = cov / math.sqrt(max(varb*sum((x-ma)**2 for x in ra)/(len(ra)-1), 1e-18))
    beta = cov/varb
    # distance via log-spread stdev
    la=[math.log(x) for x in prices_a if x>0]
    lb=[math.log(x) for x in prices_b if x>0]
    n=min(len(la),len(lb))
    if n<30: return (corr,beta,None)
    spread=[la[i]-beta*lb[i] for i in range(n)]
    m=mean(spread)
    var=max(mean([(s-m)**2 for s in spread]), 1e-18)
    dist=math.sqrt(var)
    return (corr,beta,dist)

async def main():
    cfg=load_cfg()
    syms=load_symbols(cfg["universe_file"], cfg.get("limit_symbols"))
    end_d=date.today()
    start_d=end_d - timedelta(days=int(cfg["lookback_days"]))

    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    async with pool.acquire() as con:
        mp = await fetch_close_map(con, syms, start_d, end_d)

    scored=[]
    for a,b in itertools.combinations(syms,2):
        A=mp.get(a) or []; B=mp.get(b) or []
        pa,pb=align_series(A,B)
        corr,beta,dist=corr_beta_and_dist(pa,pb)
        if corr is None or beta is None or math.isnan(corr) or math.isnan(beta): continue
        if corr < float(cfg["min_corr"]): continue
        if dist is None or math.isnan(dist) or dist<=0: continue
        scored.append((corr, dist, a, b, beta))

    # rank by corr DESC then dist ASC; keep top_k
    scored.sort(key=lambda x: (-x[0], x[1]))
    top=scored[: int(cfg["top_k"])]

    async with pool.acquire() as con:
        for corr,dist,a,b,beta in top:
            a1,b1 = sorted([a,b])
            r=await con.fetchrow(
              "INSERT INTO pairs_universe(a_symbol,b_symbol,beta,active,note) "
              "VALUES($1,$2,$3,true,$4) "
              "ON CONFLICT (LEAST(a_symbol,b_symbol), GREATEST(a_symbol,b_symbol)) DO UPDATE SET beta=EXCLUDED.beta, active=true "
              "RETURNING pair_id",
              a1,b1,float(beta), f"corr={corr:.3f}, dist={dist:.4f}"
            )
            pid=int(r["pair_id"])
            await con.execute(
              "INSERT INTO pairs_stats(pair_id,corr_lookback_days,corr,dist_metric) VALUES($1,$2,$3,$4)",
              pid, int(cfg["lookback_days"]), float(corr), float(dist)
            )
    await pool.close()
    print(f"[pairs] upserted {len(top)} pairs.")
    for corr,dist,a,b,beta in top[:10]:
        print(f"  {a}-{b} corr={corr:.2f} beta={beta:.2f} dist={dist:.4f}")

if __name__=="__main__":
    import asyncio; asyncio.run(main())
