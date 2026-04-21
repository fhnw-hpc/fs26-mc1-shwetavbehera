"""
Part 2 - Producer Experiment
Produces stock ticks to a configurable topic at 10Hz.

Usage:
   python producers/producer_experiment.py <topic>

Examples:
   python producers/producer_experiment.py stock-ticks-p1-r1
   python producers/producer_experiment.py stock-ticks-p3-r1
   python producers/producer_experiment.py stock-ticks-p3-r3

Run via Docker:
   docker exec -it jupyter1 python producers/producer_experiment.py stock-ticks-p3-r3
"""

import sys
import time
import random
from datetime import datetime, timezone

import msgpack
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"

topic = sys.argv[1] if len(sys.argv) > 1 else "stock-ticks-p3-r3"

STOCKS = {"AAPL": 182.0, "GOOGL": 140.0, "MSFT": 415.0, "TSLA": 175.0, "AMZN": 185.0}


def serialize(data: dict) -> bytes:
    return msgpack.packb(data, use_bin_type=True)


def next_price(current: float) -> float:
    return round(max(1.0, current + random.uniform(-0.5, 0.5)), 2)


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=serialize,
)

print(f"Producing to topic '{topic}' at 10Hz. Ctrl+C to stop.")
prices = dict(STOCKS)

try:
    while True:
        symbol = random.choice(list(prices.keys()))
        prices[symbol] = next_price(prices[symbol])
        message = {
            "symbol": symbol,
            "price": prices[symbol],
            "volume": random.randint(10, 500),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        # Use symbol as message key so same symbol always goes to same partition
        producer.send(topic, key=symbol.encode(), value=message)
        print(f"[{topic}] {message}")
        time.sleep(0.1)
except KeyboardInterrupt:
    print("Stopping producer.")
finally:
    producer.flush()
    producer.close()