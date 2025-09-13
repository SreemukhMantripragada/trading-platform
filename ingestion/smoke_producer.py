"""
This script produces synthetic tick data messages to a Kafka topic for testing or development purposes.

Modules:
    - asyncio: For asynchronous event loop management.
    - os: For environment variable access.
    - time: For timestamp generation.
    - random: For generating random price and volume data.
    - ujson as json: For fast JSON serialization.
    - aiokafka.AIOKafkaProducer: For asynchronous Kafka message production.

Environment Variables:
    - KAFKA_BROKER: Kafka broker address (default: "localhost:9092").
    - TOPIC: Kafka topic to publish messages to (default: "ticks").
    - SYMS: Comma-separated list of symbols to generate ticks for (default: "RELIANCE,TCS").

Functions:
    - now_ns(): Returns the current time in nanoseconds.

Main Logic:
    - Initializes an asynchronous Kafka producer.
    - Generates 20 ticks per symbol, each tick containing:
        - symbol: The stock symbol.
        - event_ts: Event timestamp in nanoseconds.
        - ltp: Last traded price (randomized around 2500).
        - vol: Trade volume (random integer between 1 and 5).
    - Sends each tick as a JSON-encoded message to the specified Kafka topic.
    - Prints the total number of ticks sent upon completion.

Usage:
    Run this script directly to produce synthetic tick data to the configured Kafka topic.
"""
import asyncio, os, time, random, ujson as json
from aiokafka import AIOKafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC  = os.getenv("TOPIC", "ticks")
SYMS   = (os.getenv("SYMS") or "RELIANCE,TCS").split(",")

def now_ns(): return time.time_ns()

async def main():
    prod = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
    await prod.start()
    try:
        base = now_ns(); n=0
        for i in range(20):
            for sym in SYMS:
                px = round(2500 + random.uniform(-2,2), 2)
                vol = random.randint(1,5)
                evt = base + i*150_000_000
                msg = {"symbol": sym, "event_ts": evt, "ltp": px, "vol": vol}
                await prod.send_and_wait(TOPIC, json.dumps(msg).encode(), key=sym.encode())
                n += 1
        print(f"Sent {n} ticks to '{TOPIC}' via {BROKER}")
    finally:
        await prod.stop()

if __name__ == "__main__":
    asyncio.run(main())
