# stability_kafka.py
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from notebooks.shared.kafka_config import BOOTSTRAP_SERVERS
from notebooks.shared.serializer import serialize

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=serialize)
TOPIC = "stock-ticks"
DURATION = 60
WINDOW = 10

start = time.time()
window_start = start
window_count = 0
total = 0

while time.time() - start < DURATION:
    producer.send(TOPIC, value={"symbol": "AAPL", "price": 180.0,
                                "volume": 100,
                                "timestamp": datetime.now(timezone.utc).isoformat()})
    window_count += 1
    total += 1
    now = time.time()
    if now - window_start >= WINDOW:
        print(f"  t={int(now-start):3d}s  window={window_count/WINDOW:.0f} msg/s  total={total}")
        window_start = now
        window_count = 0

producer.flush()
producer.close()