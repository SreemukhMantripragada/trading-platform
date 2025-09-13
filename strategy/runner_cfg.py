import os, asyncio, ujson as json, yaml
from collections import deque, defaultdict
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg

BROKER=os.getenv("KAFKA_BROKER","localhost:9092"); IN_TOPIC=os.getenv("IN_TOPIC","bars.1m"); OUT_TOPIC="orders"; GROUP_ID="strategy_runner_cfg"
PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432")); PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
CONF=os.getenv("STRAT_CONF","strategy/registry.yaml")

class SMA_Cross:
    name="SMA_Cross"
    def __init__(self, fast=5, slow=20, qty=1, bucket="LOW"):
        assert fast<slow; self.fast=fast; self.slow=slow; self.qty=qty; self.bucket=bucket
        self.pr=defaultdict(lambda: deque(maxlen=slow)); self.last=defaultdict(int)
    def on_bar(self,s,ts,o,h,l,c,v):
        dq=self.pr[s]; dq.append(float(c))
        if len(dq)<self.slow: return None
        slow=sum(dq)/self.slow; fast=sum(list(dq)[-self.fast:])/self.fast
        st=1 if fast>slow else (-1 if fast<slow else 0); pv=self.last[s]; self.last[s]=st
        if pv<=0 and st>0: return {"side":"BUY","qty":self.qty,"bucket":self.bucket,"why":"sma_up"}
        if pv>=0 and st<0: return {"side":"SELL","qty":self.qty,"bucket":self.bucket,"why":"sma_dn"}
        return None

class RSI_Reversion:
    name="RSI_Reversion"
    def __init__(self, period=14, buy_th=30, sell_th=70, qty=1, bucket="MED"):
        self.p=period; self.bt=buy_th; self.st=sell_th; self.qty=qty; self.bucket=bucket
        self.cl=defaultdict(lambda: deque(maxlen=period+1))
    def _rsi(self,dq):
        if len(dq)<self.p+1: return None
        g=l=0.0
        for i in range(1,len(dq)):
            d=dq[i]-dq[i-1]
            if d>0:g+=d
            elif d<0:l+=-d
        if l==0: return 100.0
        rs=g/self.p/(l/self.p); return 100.0 - 100.0/(1.0+rs)
    def on_bar(self,s,ts,o,h,l,c,v):
        dq=self.cl[s]; dq.append(float(c)); r=self._rsi(dq)
        if r is None: return None
        if r<self.bt: return {"side":"BUY","qty":self.qty,"bucket":self.bucket,"why":f"rsi={r:.1f}<bt"}
        if r>self.st: return {"side":"SELL","qty":self.qty,"bucket":self.bucket,"why":f"rsi={r:.1f}>st"}
        return None

def mk_coid(st, sym, ts): return f"{st}:{sym}:{ts}"

async def main():
    conf=yaml.safe_load(open(CONF))
    models=[]
    for s in conf["strategies"]:
        k=s["kind"]; p=s.get("params",{})
        if k=="SMA_Cross": models.append(SMA_Cross(**p))
        elif k=="RSI_Reversion": models.append(RSI_Reversion(**p))
    pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    cons=AIOKafkaConsumer(IN_TOPIC,bootstrap_servers=BROKER,enable_auto_commit=False,auto_offset_reset="earliest",group_id=GROUP_ID,value_deserializer=lambda b: json.loads(b.decode()),key_deserializer=lambda b: b.decode() if b else None)
    prod=AIOKafkaProducer(bootstrap_servers=BROKER,acks="all",linger_ms=5)
    await cons.start(); await prod.start()
    try:
        while True:
            batches=await cons.getmany(timeout_ms=1000,max_records=2000)
            for _,msgs in batches.items():
                for m in msgs:
                    r=m.value; sym=r["symbol"]; ts=int(r["ts"]); o,h,l,c=float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"]); v=int(r.get("vol") or 0)
                    for mdl in models:
                        sig=mdl.on_bar(sym,ts,o,h,l,c,v)
                        if not sig: continue
                        ord={"client_order_id": mk_coid(mdl.name,sym,ts),"ts":ts,"symbol":sym,"side":sig["side"],"qty":sig["qty"],"order_type":"MKT","strategy":mdl.name,"reason":sig["why"],"risk_bucket":sig["bucket"],"status":"NEW"}
                        async with pool.acquire() as con:
                            await con.execute("INSERT INTO orders(client_order_id, ts, symbol, side, qty, order_type, strategy, reason, risk_bucket, status) VALUES($1,to_timestamp($2),$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (client_order_id) DO NOTHING", ord["client_order_id"],ord["ts"],ord["symbol"],ord["side"],ord["qty"],ord["order_type"],ord["strategy"],ord["reason"],ord["risk_bucket"],ord["status"])
                        await prod.send_and_wait(OUT_TOPIC, json.dumps(ord).encode(), key=sym.encode())
            await cons.commit()
    finally:
        await cons.stop(); await prod.stop(); await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
