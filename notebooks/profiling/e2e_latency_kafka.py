import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import time
import statistics
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer
from shared.kafka_config import BOOTSTRAP_SERVERS
from shared.serializer import serialize, deserialize

TOPIC = "stock-ticks"
N = 200

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=serialize,
)

# Start consumer before publishing so it doesn't miss messages.
# Use a unique group so it doesn't interfere with running sinks.
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="e2e-latency-test-v2",
    value_deserializer=deserialize,
    auto_offset_reset="latest",
    consumer_timeout_ms=8000,
    fetch_max_wait_ms=10,
    fetch_min_bytes=1,
)

# Trigger partition assignment by doing a dummy poll before publishing.
# Without this the consumer is not yet assigned and will miss early messages.
consumer.poll(timeout_ms=2000)
time.sleep(0.5)

# Warm up the producer (first send triggers metadata fetch).
for _ in range(5):
    producer.send(TOPIC, value={
        "symbol": "AAPL", "price": 180.0, "volume": 100,
        "t_sent": time.time(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
producer.flush()
time.sleep(0.5)

# Now send N tagged messages one at a time with flush each time.
# flush() ensures each message is actually delivered before we move on.
for _ in range(N):
    producer.send(TOPIC, value={
        "symbol": "AAPL", "price": 180.0, "volume": 100,
        "t_sent": time.time(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    producer.flush()

# Collect N latency measurements.
latencies = []
for msg in consumer:
    val = msg.value
    if "t_sent" not in val:
        continue
    latency_ms = (time.time() - val["t_sent"]) * 1000
    latencies.append(latency_ms)
    if len(latencies) >= N:
        break

producer.close()
consumer.close()

if not latencies:
    print("No messages received — check that the topic exists and brokers are reachable.")
    sys.exit(1)

print(f"Kafka E2E Latency ({len(latencies)} messages)")
print(f"  mean  = {statistics.mean(latencies):.2f} ms")
print(f"  stdev = {statistics.stdev(latencies):.2f} ms")
print(f"  min   = {min(latencies):.2f} ms")
print(f"  max   = {max(latencies):.2f} ms")
print(f"  p95   = {sorted(latencies)[int(0.95 * len(latencies))]:.2f} ms")