import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import csv
from shared.rmq_config import EXCHANGES
from shared.rmq_helpers import get_connection, declare_exchange, declare_queue
from shared.serializer import deserialize

EXCHANGE = EXCHANGES["stock_ticks"]
QUEUE    = "stock-ticks-sink"
OUTPUT_FILE = "/app/data/stock_ticks_rmq.csv"
FIELDS = ["symbol", "price", "volume", "timestamp"]


def run():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    file_exists = os.path.isfile(OUTPUT_FILE)

    connection = get_connection()
    channel = connection.channel()
    declare_exchange(channel, EXCHANGE)
    declare_queue(channel, QUEUE, EXCHANGE)

    # Only fetch one message at a time — don't overwhelm the consumer
    channel.basic_qos(prefetch_count=1)

    print(f"RabbitMQ stock sink listening on queue '{QUEUE}'...")

    def callback(ch, method, properties, body):
        message = deserialize(body)
        row = {k: message.get(k) for k in FIELDS}

        with open(OUTPUT_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        print(f"[SINK-STOCKS] {row}")
        # Acknowledge the message — tells RabbitMQ it can delete it
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Stopping stock sink.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    run()