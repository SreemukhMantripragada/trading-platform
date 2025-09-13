import os, asyncio, ujson as json, pyarrow as pa, pyarrow.parquet as pq, time
from aiokafka import AIOKafkaConsumer
BROKER=os.getenv("KAFKA_BROKER","localhost:9092"); TOPIC=os.getenv("TOPIC","dlq"); OUT=os.getenv("OUT","ingestion/dlq_out")
os.makedirs(OUT, exist_ok=True)
async def main():
    cons=AIOKafkaConsumer(TOPIC, bootstrap_servers=BROKER, enable_auto_commit=True, auto_offset_reset="earliest", group_id="dlq_sink", value_deserializer=lambda b: json.loads(b.decode()))
    await cons.start()
    try:
        buf=[]
        while True:
            batches=await cons.getmany(timeout_ms=1000, max_records=1000)
            for _,msgs in batches.items():
                for m in msgs: buf.append(m.value)
            if len(buf)>=100:
                ts=int(time.time()); path=f"{OUT}/dlq_{ts}.parquet"
                tbl=pa.Table.from_pylist(buf); pq.write_table(tbl, path); print("DLQ wrote", path); buf.clear()
    finally: await cons.stop()
if __name__=="__main__":
    import asyncio; asyncio.run(main())
