# backtest/pairs_backtest.py  (replace if you want TF support)
import os, yaml, asyncpg, csv, numpy as np
from datetime import datetime, timedelta, timezone

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
PAIRS_YAML="configs/pairs.yaml"; OUT_CSV="data/historical/pairs_bt_results.csv"
TF_MAP={"1m":1,"3m":3,"5m":5,"15m":15}

async def fetch_series(con, sym, start, end):
    rows = await con.fetch("SELECT ts,c FROM bars_1m_golden WHERE symbol=$1 AND ts BETWEEN $2 AND $3 ORDER BY ts", sym, start, end)
    ts = [int(r["ts"].timestamp()) for r in rows]
    px = np.array([float(r["c"]) for r in rows], dtype=np.float64)
    return np.array(ts,dtype=np.int64), px

def resample_close(ts_sec, px, tfm):
    if tfm==1: return ts_sec, px
    step=tfm*60; buckets={}
    for t,p in zip(ts_sec,px): b=t-(t%step); buckets[b]=p
    if not buckets: return np.array([],dtype=np.int64), np.array([],dtype=np.float64)
    bts=np.array(sorted(buckets.keys()),dtype=np.int64); bp=np.array([buckets[t] for t in bts],dtype=np.float64)
    return bts, bp

def align(ts1, px1, ts2, px2):
    i1={t:i for i,t in enumerate(ts1)}; i2={t:i for i,t in enumerate(ts2)}
    common=sorted(set(i1)&set(i2))
    if len(common)<200: return None,None,None
    x=np.array([px1[i1[t]] for t in common],dtype=np.float64)
    y=np.array([px2[i2[t]] for t in common],dtype=np.float64)
    return np.array(common,dtype=np.int64), x, y

def ols_beta(x,y):
    x1=np.vstack([np.ones_like(x),x]).T
    return float(np.linalg.lstsq(x1,y,rcond=None)[0][1])

def zseries(x,y,lookback,mode,fixed_beta,beta_lb):
    z=[]; bet=[]; sp=[]
    for i in range(len(x)):
        if mode=="dynamic":
            j=max(0,i-beta_lb+1)
            b=fixed_beta if i-j+1<10 else ols_beta(x[j:i+1],y[j:i+1])
        else:
            b=fixed_beta
        bet.append(b); s=y[i]-b*x[i]; sp.append(s)
        j2=max(0,i-lookback+1); w=sp[j2:i+1]
        if len(w)>=2:
            m=float(np.mean(w)); sd=float(np.std(w,ddof=1)) if len(w)>1 else 0.0
            z.append(0.0 if sd==0 else (s-m)/sd)
        else: z.append(0.0)
    return np.array(z), np.array(bet), np.array(sp)

async def main():
    with open(PAIRS_YAML,"r") as f:
        cfg=yaml.safe_load(f)
    pairs=cfg["pairs"]; defaults=cfg.get("defaults",{})
    start=datetime.now(timezone.utc)-timedelta(days=60); end=datetime.now(timezone.utc)
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    out=[]
    async with pool.acquire() as con:
        for p in pairs:
            tf=p.get("tf",defaults.get("default_tf","1m")); tfm=TF_MAP[tf]
            tx, px = await fetch_series(con,p["leg_x"],start,end)
            ty, py = await fetch_series(con,p["leg_y"],start,end)
            if px.size==0 or py.size==0: continue
            rtx,rpx=resample_close(tx,px,tfm); rty,rpy=resample_close(ty,py,tfm)
            ts,x,y = align(rtx,rpx,rty,rpy)
            if ts is None: 
                print(f"[bt] skip {p['leg_x']}-{p['leg_y']} tf={tf} (low overlap)")
                continue
            z,bets,sp=zseries(x,y,p["lookback"],p["hedge_mode"],p["fixed_beta"],p["beta_lookback"])
            entry,exit,stop=p["entry_z"],p["exit_z"],p["stop_z"]
            pos=None; trades=[]; eq=0.0
            for i,t in enumerate(ts):
                zi=z[i]
                if pos is None and abs(zi)>=entry:
                    pos={"side":"SHORTSPREAD" if zi>0 else "LONGSPREAD","i":i,"s0":sp[i]}
                elif pos is not None and (abs(zi)<=exit or abs(zi)>=stop or (i-pos["i"])>=p.get("max_hold_min",240)/ (tfm)): 
                    pnl=(sp[i]-pos["s0"]); 
                    if pos["side"]=="SHORTSPREAD": pnl=-pnl
                    trades.append(pnl); pos=None
            out.append({"pair":f"{p['leg_x']}-{p['leg_y']}", "tf":tf, "n":len(ts), "trades":len(trades),
                        "pnl_sum":float(np.sum(trades) if trades else 0.0),
                        "pnl_avg":float(np.mean(trades) if trades else 0.0) if trades else 0.0})
            print(f"[bt] {p['leg_x']}-{p['leg_y']} tf={tf} trades={len(trades)} pnl_sum={out[-1]['pnl_sum']:.2f}")
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    import csv as _csv
    with open(OUT_CSV,"w",newline="") as f:
        fn = ["pair","tf","n","trades","pnl_sum","pnl_avg"]
        w=_csv.DictWriter(f,fieldnames=fn); w.writeheader(); [w.writerow(r) for r in out]
    print(f"[bt] wrote {OUT_CSV}")

if __name__=="__main__":
    import asyncio; asyncio.run(main())
