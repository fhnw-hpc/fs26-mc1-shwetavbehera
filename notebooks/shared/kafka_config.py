import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"

TOPICS = {
    "stock_ticks":   "stock-ticks",
    "news_posts":    "news-posts",
    "trade_signals": "trade-signals",
}


def wait_for_kafka(retries: int = 15, delay: int = 4):
    """
    Block until Kafka brokers are reachable.
    Called at the top of every producer/consumer so Docker services
    don't crash on startup before the brokers are ready.
    """
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
            p.close()
            print(f"Kafka ready (attempt {attempt})")
            return
        except NoBrokersAvailable:
            print(f"Kafka not ready yet (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts.")