"""
Sends a couple of orders to 'orders' for OMS/EMS/ledger end-to-end test.
"""
import os, asyncio, time, ujson as json
from aiokafka import AIOKafkaProducer

BROKER=os.getenv("KAFKA_BROKER","localhost:9092")
TOPIC=os.getenv("ORD_TOPIC","orders")

async def main():
    p=AIOKafkaProducer(bootstrap_servers=BROKER, acks="all")
    await p.start()
    try:
        now=int(time.time())
        base={
          "order_type":"MKT","strategy":"SMOKE","risk_bucket":"LOW",
          "extra":{"costs_est":{"est_fill_price":2500.0,"est_total_cost_inr":12.0}}
        }
        orders=[
          {"client_order_id":f"SMOKE:RELIANCE:{now}:BUY","ts":now,"symbol":"RELIANCE","side":"BUY","qty":50, **base},
          {"client_order_id":f"SMOKE:TCS:{now+1}:SELL","ts":now+1,"symbol":"TCS","side":"SELL","qty":10, **base},
        ]
        for o in orders:
            await p.send_and_wait(TOPIC, json.dumps(o).encode(), key=o["symbol"].encode())
        print(f"Sent {len(orders)} orders to {TOPIC}")
    finally:
        await p.stop()

if __name__=="__main__":
    asyncio.run(main())
