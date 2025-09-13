import os, csv, asyncpg, asyncio
PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading") 
PG_USER=os.getenv("POSTGRES_USER","trader") 
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

UPSERT="""INSERT INTO bars_1m(symbol,ts,o,h,l,c,vol,n_trades)
VALUES($1,$2,$3,$4,$5,$6,$7,COALESCE($8,0))
ON CONFLICT(symbol,ts) DO UPDATE SET
o=EXCLUDED.o,h=EXCLUDED.h,l=EXCLUDED.l,c=EXCLUDED.c,vol=bars_1m.vol+EXCLUDED.vol,n_trades=bars_1m.n_trades+EXCLUDED.n_trades"""

async def load(symbol:str, path:str):
    pool = await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    try:
        rows=[]
        with open(path) as f:
            r=csv.DictReader(f)
            for row in r:
                rows.append((symbol, row["ts"], float(row["o"]),float(row["h"]),float(row["l"]),float(row["c"]), int(row["vol"]), 1))
        async with pool.acquire() as con:
            async with con.transaction():
                await con.executemany(UPSERT, rows)
        print(f"[backfill] {symbol} <- {len(rows)} rows from {path}")
    finally:
        await pool.close()

if __name__=="__main__":
    import sys, asyncio
    if len(sys.argv)<3: raise SystemExit("usage: SYMBOL CSV_PATH")
    asyncio.run(load(sys.argv[1], sys.argv[2]))
