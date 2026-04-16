import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from shared.kafka_config import BOOTSTRAP_SERVERS, TOPICS
from shared.serializer import serialize

TOPIC = TOPICS["stock_ticks"]

STOCKS = {
    "AAPL": 182.0,
    "GOOGL": 140.0,
    "MSFT": 415.0,
    "TSLA": 175.0,
    "AMZN": 185.0,
}


def next_price(current: float) -> float:
    """Simulate a small random walk from the current price."""
    change = random.uniform(-0.5, 0.5)
    return round(max(1.0, current + change), 2)


def run():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize,
    )

    print(f"Starting stock tick producer on topic '{TOPIC}' at 10Hz...")

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

            producer.send(TOPIC, value=message)
            print(f"[STOCK] {message}")

            time.sleep(0.1)  # 10Hz

    except KeyboardInterrupt:
        print("Stopping stock producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()