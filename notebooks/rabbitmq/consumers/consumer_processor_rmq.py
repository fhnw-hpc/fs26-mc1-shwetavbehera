import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pika
from datetime import datetime, timezone
from shared.rmq_config import EXCHANGES
from shared.rmq_helpers import get_connection, declare_exchange, declare_queue
from shared.serializer import serialize, deserialize

INPUT_EXCHANGE  = EXCHANGES["news_posts"]
INPUT_QUEUE     = "news-posts-processor"
OUTPUT_EXCHANGE = EXCHANGES["trade_signals"]

KNOWN_SYMBOLS = {"AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"}


def detect_symbols(tags: list) -> list:
    return [tag for tag in tags if tag in KNOWN_SYMBOLS]


def run():
    connection = get_connection()
    channel = connection.channel()

    # Declare both exchanges and the input queue
    declare_exchange(channel, INPUT_EXCHANGE)
    declare_exchange(channel, OUTPUT_EXCHANGE)
    declare_queue(channel, INPUT_QUEUE, INPUT_EXCHANGE)

    channel.basic_qos(prefetch_count=1)

    print(f"RabbitMQ processor listening on '{INPUT_QUEUE}', publishing to '{OUTPUT_EXCHANGE}'...")

    def callback(ch, method, properties, body):
        post = deserialize(body)
        symbols = detect_symbols(post.get("tags", []))

        if symbols:
            for symbol in symbols:
                signal = {
                    "symbol": symbol,
                    "direction": "BUY" if post["sentiment_hint"] > 0 else "SELL",
                    "confidence": abs(post["sentiment_hint"]),
                    "source_post_id": post["post_id"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                ch.basic_publish(
                    exchange=OUTPUT_EXCHANGE,
                    routing_key="",
                    body=serialize(signal),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                print(f"[SIGNAL] {signal}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=INPUT_QUEUE, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Stopping processor.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    run()