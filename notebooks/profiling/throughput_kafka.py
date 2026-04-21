# throughput_kafka.py
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from notebooks.shared.kafka_config import BOOTSTRAP_SERVERS
from notebooks.shared.serializer import serialize

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=serialize)
TOPIC = "stock-ticks"
count = 0
start = time.time()
DURATION = 30  # seconds

while time.time() - start < DURATION:
    producer.send(TOPIC, value={"symbol": "AAPL", "price": 180.0,
                                "volume": 100,
                                "timestamp": datetime.now(timezone.utc).isoformat()})
    count += 1

producer.flush()
elapsed = time.time() - start
print(f"Kafka throughput: {count} messages in {elapsed:.1f}s = {count/elapsed:.0f} msg/s")
producer.close()