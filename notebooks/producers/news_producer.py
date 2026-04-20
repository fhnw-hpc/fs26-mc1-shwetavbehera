import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from shared.kafka_config import BOOTSTRAP_SERVERS, TOPICS, wait_for_kafka
from shared.serializer import serialize

TOPIC = TOPICS["news_posts"]

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
    wait_for_kafka()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize,
    )

    print(f"Starting news producer on topic '{TOPIC}' at ~1Hz...")

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
            producer.send(TOPIC, value=message)
            print(f"[NEWS] {message}")
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("Stopping news producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()