import sys
import os


# Make both shared modules findable from the Jupyter container
sys.path.insert(0, "/home/jovyan")
sys.path.insert(0, "/home/jovyan/rabbitmq")

import time
import pika
import statistics
import msgpack

# Inline the RabbitMQ config and helpers to avoid the circular import
# between rabbitmq/shared/rmq_helpers.py and shared/rmq_config.py
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq1")

EXCHANGES = {
    "stock_ticks":   "stock-ticks",
    "news_posts":    "news-posts",
    "trade_signals": "trade-signals",
}

def serialize(data):
    return msgpack.packb(data, use_bin_type=True)

def deserialize(data):
    return msgpack.unpackb(data, raw=False)

def get_connection(retries=10, delay=3):
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    for attempt in range(1, retries + 1):
        try:
            conn = pika.BlockingConnection(params)
            print(f"Connected to RabbitMQ at '{RABBITMQ_HOST}' (attempt {attempt})")
            return conn
        except pika.exceptions.AMQPConnectionError:
            print(f"Not ready (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to RabbitMQ after {retries} attempts.")


EXCHANGE = EXCHANGES["stock_ticks"]
QUEUE    = "e2e-latency-test"
N        = 200

conn = get_connection()
ch   = conn.channel()

# Declare exchange and a temporary auto-delete queue
ch.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
ch.queue_declare(queue=QUEUE, durable=False, auto_delete=True)
ch.queue_bind(queue=QUEUE, exchange=EXCHANGE)
ch.basic_qos(prefetch_count=1)

latencies = []

def callback(c, method, props, body):
    msg = deserialize(body)
    if "t_sent" in msg:
        latencies.append((time.time() - msg["t_sent"]) * 1000)
    c.basic_ack(delivery_tag=method.delivery_tag)
    if len(latencies) >= N:
        c.stop_consuming()

ch.basic_consume(queue=QUEUE, on_message_callback=callback)

# Publish N messages on a second channel (same connection is fine for BlockingConnection)
pub_ch = conn.channel()
for _ in range(N):
    pub_ch.basic_publish(
        exchange=EXCHANGE,
        routing_key="",
        body=serialize({
            "symbol": "AAPL",
            "price":  180.0,
            "volume": 100,
            "t_sent": time.time(),
        }),
        properties=pika.BasicProperties(delivery_mode=2),
    )

# Now consume — messages are already queued so this returns quickly
ch.start_consuming()

conn.close()

if not latencies:
    print("No messages received — check RABBITMQ_HOST and that the stack is running.")
    sys.exit(1)

print(f"\nRabbitMQ E2E Latency ({len(latencies)} messages)")
print(f"  mean  = {statistics.mean(latencies):.2f} ms")
print(f"  stdev = {statistics.stdev(latencies):.2f} ms")
print(f"  min   = {min(latencies):.2f} ms")
print(f"  max   = {max(latencies):.2f} ms")
print(f"  p95   = {sorted(latencies)[int(0.95 * len(latencies))]:.2f} ms")