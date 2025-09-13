import os, asyncpg, asyncio
PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432")); PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")
SQL="""INSERT INTO eod_summary(as_of,orders,fills,symbols,realized_pnl)
SELECT CURRENT_DATE,
       (SELECT count(*) FROM orders WHERE ts::date=CURRENT_DATE),
       (SELECT count(*) FROM fills  WHERE ts::date=CURRENT_DATE),
       (SELECT count(DISTINCT symbol) FROM blotter WHERE order_ts::date=CURRENT_DATE),
       COALESCE((SELECT sum(realized_pnl) FROM positions),0)
ON CONFLICT (as_of) DO UPDATE SET orders=EXCLUDED.orders, fills=EXCLUDED.fills, symbols=EXCLUDED.symbols, realized_pnl=EXCLUDED.realized_pnl"""
async def main():
    con=await asyncpg.connect(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
    await con.execute(SQL); await con.close(); print("EOD summary upserted")
if __name__=="__main__": asyncio.run(main())
