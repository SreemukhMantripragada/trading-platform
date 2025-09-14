"""
compute/sinks/s3_archiver.py
Dumps bars_1m for a day to Parquet (partitioned) and uploads to S3/MinIO.
Also prunes local archive older than RETAIN_DAYS.

ENV:
  ARCHIVE_DIR=data/archive/bars_1m
  DAY=YYYY-MM-DD (default: yesterday)
  RETAIN_DAYS=100
  S3_BUCKET=trading-archive
  S3_PREFIX=bars_1m
  S3_ENDPOINT_URL= (use for MinIO, e.g. http://localhost:9000)
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
Run:
  python compute/sinks/s3_archiver.py
"""
from __future__ import annotations
import os, asyncio, asyncpg, pyarrow as pa, pyarrow.parquet as pq, boto3, shutil
from datetime import date, timedelta, datetime
from pathlib import Path

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

ARCHIVE_DIR=Path(os.getenv("ARCHIVE_DIR","data/archive/bars_1m"))
DAY=os.getenv("DAY") or str(date.today() - timedelta(days=1))
RETAIN_DAYS=int(os.getenv("RETAIN_DAYS","100"))

S3_BUCKET=os.getenv("S3_BUCKET","")
S3_PREFIX=os.getenv("S3_PREFIX","bars_1m")
S3_ENDPOINT=os.getenv("S3_ENDPOINT_URL","")

async def fetch_day(con, day:str):
    rows = await con.fetch("""
      SELECT symbol, ts, o,h,l,c,vol,n_trades
      FROM bars_1m
      WHERE ts::date = $1::date
      ORDER BY symbol, ts
    """, day)
    return rows

def write_parquet(day:str, rows):
    daydir=ARCHIVE_DIR / day
    daydir.mkdir(parents=True, exist_ok=True)
    # write per-symbol files
    cur_sym=None; buf=[]
    def flush():
        if not buf: return
        tbl=pa.table({
            "symbol":[r["symbol"] for r in buf],
            "ts"    :[r["ts"] for r in buf],
            "o"     :[r["o"]  for r in buf],
            "h"     :[r["h"]  for r in buf],
            "l"     :[r["l"]  for r in buf],
            "c"     :[r["c"]  for r in buf],
            "vol"   :[r["vol"] for r in buf],
            "n_trades":[r["n_trades"] for r in buf],
        })
        pq.write_table(tbl, daydir / f"{cur_sym}.parquet", compression="zstd")
    for r in rows:
        s=r["symbol"]
        if cur_sym is None: cur_sym=s
        if s != cur_sym:
            flush(); buf=[]; cur_sym=s
        buf.append(r)
    flush()
    return daydir

def s3_upload_dir(daydir:Path):
    if not S3_BUCKET: return
    s3=boto3.client("s3", endpoint_url=S3_ENDPOINT or None)
    for p in daydir.glob("*.parquet"):
        key=f"{S3_PREFIX}/{daydir.name}/{p.name}"
        s3.upload_file(str(p), S3_BUCKET, key)
        print(f"[archiver] uploaded s3://{S3_BUCKET}/{key}")

def prune_local():
    if not ARCHIVE_DIR.exists(): return
    cutoff = datetime.utcnow() - timedelta(days=RETAIN_DAYS)
    for d in sorted(ARCHIVE_DIR.iterdir()):
        try:
            dt = datetime.strptime(d.name, "%Y-%m-%d")
        except: 
            continue
        if dt < cutoff:
            shutil.rmtree(d, ignore_errors=True)
            print(f"[archiver] pruned {d}")

async def main():
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        async with pool.acquire() as con:
            rows=await fetch_day(con, DAY)
        daydir = write_parquet(DAY, rows)
        s3_upload_dir(daydir)
        prune_local()
        print(f"[archiver] archived {DAY}: {len(rows)} rows")
    finally:
        await pool.close()

if __name__=="__main__":
    asyncio.run(main())
