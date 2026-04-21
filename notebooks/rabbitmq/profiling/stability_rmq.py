# stability_rmq.py
import time, pika
from rabbitmq.shared.rmq_helpers import get_connection, declare_exchange
from rabbitmq.shared.rmq_config import EXCHANGES
from rabbitmq.shared.serializer import serialize

conn = get_connection()
ch = conn.channel()
EXCHANGE = EXCHANGES["stock_ticks"]
declare_exchange(ch, EXCHANGE)
DURATION = 60
WINDOW = 10

start = time.time()
window_start = start
window_count = 0
total = 0

while time.time() - start < DURATION:
    ch.basic_publish(exchange=EXCHANGE, routing_key="",
                     body=serialize({"symbol": "AAPL", "price": 180.0, "volume": 100}),
                     properties=pika.BasicProperties(delivery_mode=2))
    window_count += 1
    total += 1
    now = time.time()
    if now - window_start >= WINDOW:
        print(f"  t={int(now-start):3d}s  window={window_count/WINDOW:.0f} msg/s  total={total}")
        window_start = now
        window_count = 0

conn.close()