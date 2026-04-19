import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from shared.kafka_config import BOOTSTRAP_SERVERS
from shared.serializer import serialize

"""
Part 2

Usage:
   python producers/producer_experiment.py stock-ticks-p3-r3
"""

topic = sys.argv[1] if len(sys.argv) > 1 else "stock-ticks-p3-r3"


STOCKS = {"AAPL": 182.0, "GOOGL": 140.0, "MSFT": 415.0, "TSLA": 175.0, "AMZN": 185.0}

def next_price(current):
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