import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from shared.kafka_config import BOOTSTRAP_SERVERS, TOPICS, wait_for_kafka
from shared.serializer import serialize, deserialize

INPUT_TOPIC  = TOPICS["news_posts"]
OUTPUT_TOPIC = TOPICS["trade_signals"]

KNOWN_SYMBOLS = {"AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"}


def detect_symbols(tags: list) -> list:
    return [tag for tag in tags if tag in KNOWN_SYMBOLS]


def run():
    wait_for_kafka()
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="signal-processor-group",
        value_deserializer=deserialize,
        auto_offset_reset="earliest",
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize,
    )

    print(f"Listening on '{INPUT_TOPIC}', emitting signals to '{OUTPUT_TOPIC}'...")

    try:
        for msg in consumer:
            post    = msg.value
            symbols = detect_symbols(post.get("tags", []))
            if not symbols:
                continue
            for symbol in symbols:
                signal = {
                    "symbol":          symbol,
                    "direction":       "BUY" if post["sentiment_hint"] > 0 else "SELL",
                    "confidence":      abs(post["sentiment_hint"]),
                    "source_post_id":  post["post_id"],
                    "timestamp":       datetime.now(timezone.utc).isoformat(),
                }
                producer.send(OUTPUT_TOPIC, value=signal)
                print(f"[SIGNAL] {signal}")

    except KeyboardInterrupt:
        print("Stopping signal processor.")
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    run()