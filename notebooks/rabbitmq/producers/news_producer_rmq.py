import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import time
import random
import uuid
from datetime import datetime, timezone
import pika
from shared.rmq_config import EXCHANGES
from shared.rmq_helpers import get_connection, declare_exchange
from shared.serializer import serialize

EXCHANGE = EXCHANGES["news_posts"]

SOURCES = ["twitter", "reddit", "bloomberg", "techcrunch"]

TEMPLATES = [
    ("{symbol} earnings beat expectations this quarter!", ["bullish", "earnings"]),
    ("Analysts downgrade {symbol} amid market uncertainty.", ["bearish", "analyst"]),
    ("Breaking: {symbol} announces new product line.", ["bullish", "product"]),
    ("{symbol} faces regulatory scrutiny in EU.", ["bearish", "regulation"]),
    ("Insider buying detected at {symbol}.", ["bullish", "insider"]),
    ("{symbol} stock hits 52-week high.", ["bullish", "momentum"]),
    ("CEO of {symbol} steps down unexpectedly.", ["bearish", "management"]),
    ("Strong demand drives {symbol} revenue growth.", ["bullish", "revenue"]),
]

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]


def run():
    connection = get_connection()
    channel = connection.channel()
    declare_exchange(channel, EXCHANGE)

    print(f"Starting RabbitMQ news producer on exchange '{EXCHANGE}' at ~1Hz...")

    try:
        while True:
            symbol = random.choice(SYMBOLS)
            template, keywords = random.choice(TEMPLATES)
            content = template.format(symbol=symbol)

            sentiment = round(random.uniform(0.3, 0.9), 2) if "bullish" in keywords \
                else round(random.uniform(-0.9, -0.3), 2)

            message = {
                "post_id": str(uuid.uuid4()),
                "source": random.choice(SOURCES),
                "author": f"user_{random.randint(100, 999)}",
                "content": content,
                "tags": [symbol] + keywords,
                "sentiment_hint": sentiment,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key="",
                body=serialize(message),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            print(f"[NEWS] {message}")
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("Stopping news producer.")
    finally:
        connection.close()


if __name__ == "__main__":
    run()