import time
import pika
from rabbitmq.shared.rmq_config import RABBITMQ_HOST


def get_connection(retries: int = 10, delay: int = 3):
    """
    Create and return a blocking RabbitMQ connection.
    Retries up to `retries` times with `delay` seconds between attempts.
    This is necessary because Docker starts all containers nearly simultaneously
    but RabbitMQ takes several seconds to be ready to accept connections.
    """
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    for attempt in range(1, retries + 1):
        try:
            connection = pika.BlockingConnection(params)
            print(f"Connected to RabbitMQ at '{RABBITMQ_HOST}' (attempt {attempt})")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            print(f"RabbitMQ not ready yet (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to RabbitMQ at '{RABBITMQ_HOST}' after {retries} attempts.")


def declare_exchange(channel, exchange_name: str):
    """Declare a fanout exchange. Idempotent — safe to call multiple times."""
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type="fanout",
        durable=True,
    )


def declare_queue(channel, queue_name: str, exchange_name: str) -> str:
    """Declare a durable queue and bind it to an exchange."""
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(queue=queue_name, exchange=exchange_name)
    return queue_name