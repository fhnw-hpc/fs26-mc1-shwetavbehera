# throughput_rmq.py
import time, pika
from rabbitmq.shared.rmq_helpers import get_connection, declare_exchange
from rabbitmq.shared.rmq_config import EXCHANGES
from rabbitmq.shared.serializer import serialize

conn = get_connection()
ch = conn.channel()
EXCHANGE = EXCHANGES["stock_ticks"]
declare_exchange(ch, EXCHANGE)
count = 0
start = time.time()
DURATION = 30

while time.time() - start < DURATION:
    ch.basic_publish(exchange=EXCHANGE, routing_key="",
                     body=serialize({"symbol": "AAPL", "price": 180.0,
                                     "volume": 100}),
                     properties=pika.BasicProperties(delivery_mode=2))
    count += 1

elapsed = time.time() - start
print(f"RabbitMQ throughput: {count} messages in {elapsed:.1f}s = {count/elapsed:.0f} msg/s")
conn.close()