import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import time
from kafka import KafkaConsumer
from shared.kafka_config import BOOTSTRAP_SERVERS
from shared.serializer import deserialize

"""
Part 2

Usage:
  python consumers/consumer_group_demo.py <group_id> <instance_name> <topic>

Examples:
  python consumers/consumer_group_demo.py group-a instance-1 stock-ticks-p3-r3
  python consumers/consumer_group_demo.py group-a instance-2 stock-ticks-p3-r3  ← same group, load balanced
  python consumers/consumer_group_demo.py group-b instance-3 stock-ticks-p3-r3  ← diff group, fan-out
"""

group_id = sys.argv[1] if len(sys.argv) > 1 else "demo-group"
instance = sys.argv[2] if len(sys.argv) > 2 else "instance-1"
topic    = sys.argv[3] if len(sys.argv) > 3 else "stock-ticks-p3-r3"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=group_id,
    value_deserializer=deserialize,
    auto_offset_reset="earliest",
)

# Give Kafka a moment to assign partitions, then print assignment
time.sleep(2)
consumer.poll(timeout_ms=100)  # triggers partition assignment
print(f"[{instance}] started · group='{group_id}' · topic='{topic}'")
print(f"[{instance}] assigned partitions: {[p.partition for p in consumer.assignment()]}")

try:
    for msg in consumer:
        print(f"[{instance}] partition={msg.partition} offset={msg.offset} → {msg.value['symbol']} ${msg.value['price']}")
except KeyboardInterrupt:
    print(f"[{instance}] stopping.")
finally:
    consumer.close()