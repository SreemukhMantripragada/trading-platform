import os, asyncpg, asyncio, pandas as pd, streamlit as st

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

async def load_top(pool, run_id=None):
    q="SELECT run_id,strategy,timeframe,(metrics->>'net_pnl')::float AS net_pnl,(metrics->>'max_dd')::float AS max_dd,(metrics->>'sharpe')::float AS sharpe,(metrics->>'trades')::int AS trades FROM backtest_results"
    if run_id: q += " WHERE run_id=$1"
    async with pool.acquire() as con:
        rows=await con.fetch(q, run_id) if run_id else await con.fetch(q)
    return pd.DataFrame([dict(r) for r in rows])

def main():
    st.title("Backtest Results")
    run = st.text_input("Run ID (optional)", "")
    async def go():
        pool=await asyncpg.create_pool(host=PG_HOST,port=PG_PORT,database=PG_DB,user=PG_USER,password=PG_PASS)
        df=await load_top(pool, int(run) if run.strip() else None); await pool.close()
        st.dataframe(df.sort_values("net_pnl", ascending=False))
    asyncio.run(go())

if __name__=="__main__":
    main()
