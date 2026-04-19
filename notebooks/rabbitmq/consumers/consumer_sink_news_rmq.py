import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import csv
from shared.rmq_config import EXCHANGES
from shared.rmq_helpers import get_connection, declare_exchange, declare_queue
from shared.serializer import deserialize

EXCHANGE = EXCHANGES["news_posts"]
QUEUE    = "news-posts-sink"
OUTPUT_FILE = "/app/data/news_posts_rmq.csv"
FIELDS = ["post_id", "source", "author", "content", "tags", "sentiment_hint", "timestamp"]


def run():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    file_exists = os.path.isfile(OUTPUT_FILE)

    connection = get_connection()
    channel = connection.channel()
    declare_exchange(channel, EXCHANGE)
    declare_queue(channel, QUEUE, EXCHANGE)

    channel.basic_qos(prefetch_count=1)

    print(f"RabbitMQ news sink listening on queue '{QUEUE}'...")

    def callback(ch, method, properties, body):
        message = deserialize(body)
        row = {k: message.get(k) for k in FIELDS}

        with open(OUTPUT_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        print(f"[SINK-NEWS] {row}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Stopping news sink.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    run()