import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import time
import random
from datetime import datetime, timezone
import pika
from shared.rmq_config import EXCHANGES
from shared.rmq_helpers import get_connection, declare_exchange
from shared.serializer import serialize

EXCHANGE = EXCHANGES["stock_ticks"]

STOCKS = {
    "AAPL": 182.0,
    "GOOGL": 140.0,
    "MSFT": 415.0,
    "TSLA": 175.0,
    "AMZN": 185.0,
}


def next_price(current: float) -> float:
    return round(max(1.0, current + random.uniform(-0.5, 0.5)), 2)


def run():
    connection = get_connection()
    channel = connection.channel()
    declare_exchange(channel, EXCHANGE)

    print(f"Starting RabbitMQ stock producer on exchange '{EXCHANGE}' at 10Hz...")
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

            # routing_key is ignored by fanout exchanges but required by the API
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key="",
                body=serialize(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                ),
            )
            print(f"[STOCK] {message}")
            time.sleep(0.1)  # 10Hz

    except KeyboardInterrupt:
        print("Stopping stock producer.")
    finally:
        connection.close()


if __name__ == "__main__":
    run()