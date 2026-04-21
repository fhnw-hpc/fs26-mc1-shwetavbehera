import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import csv
from kafka import KafkaConsumer
from shared.kafka_config import BOOTSTRAP_SERVERS, TOPICS
from shared.serializer import deserialize

TOPIC = TOPICS["stock_ticks"]
OUTPUT_FILE = "/home/jovyan/data/stock_ticks.csv"
FIELDS = ["symbol", "price", "volume", "timestamp"]


def run():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    file_exists = os.path.isfile(OUTPUT_FILE)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="stock-sink-group",
        value_deserializer=deserialize,
        auto_offset_reset="earliest",
    )

    print(f"Writing stock ticks to '{OUTPUT_FILE}'...")

    try:
        with open(OUTPUT_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if not file_exists:
                writer.writeheader()

            for msg in consumer:
                row = {k: msg.value.get(k) for k in FIELDS}
                writer.writerow(row)
                f.flush()
                print(f"[SINK-STOCKS] {row}")

    except KeyboardInterrupt:
        print("Stopping stock sink.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()