"""
Part 2 - Consumer Group Demo
Demonstrates fan-out and load-balancing consumer group patterns.

Usage:
  python consumers/consumer_group_demo.py <group_id> <instance_name> [topic]

--- Experiment 1: Fan-out ---
Two consumers in DIFFERENT groups both receive every message independently.

  Terminal 1: docker exec -it jupyter1 python consumers/consumer_group_demo.py group-a instance-1 stock-ticks-p3-r3
  Terminal 2: docker exec -it jupyter1 python consumers/consumer_group_demo.py group-b instance-2 stock-ticks-p3-r3

--- Experiment 2: Load balancing (same group, blocked by 1 partition) ---
Two consumers in the SAME group - with 1 partition only one gets messages.

  Terminal 1: docker exec -it jupyter1 python consumers/consumer_group_demo.py group-a instance-1 stock-ticks-p3-r3
  Terminal 2: docker exec -it jupyter1 python consumers/consumer_group_demo.py group-a instance-2 stock-ticks-p3-r3

--- Experiment 3: Partition parallelism (3 consumers, 3 partitions) ---
Three consumers in the SAME group against a 3-partition topic - each gets one partition.

  Terminal 1: docker exec -it jupyter1 python producers/producer_experiment.py stock-ticks-p3-r3
  Terminal 2: docker exec -it jupyter1 python consumers/consumer_group_demo.py group-p3 inst-1 stock-ticks-p3-r3
  Terminal 3: docker exec -it jupyter1 python consumers/consumer_group_demo.py group-p3 inst-2 stock-ticks-p3-r3
  Terminal 4: docker exec -it jupyter1 python consumers/consumer_group_demo.py group-p3 inst-3 stock-ticks-p3-r3
"""

import sys
import time

import msgpack
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"

group_id = sys.argv[1] if len(sys.argv) > 1 else "demo-group"
instance = sys.argv[2] if len(sys.argv) > 2 else "instance-1"
topic    = sys.argv[3] if len(sys.argv) > 3 else "stock-ticks-p3-r3"


def deserialize(data: bytes) -> dict:
    return msgpack.unpackb(data, raw=False)


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
        print(
            f"[{instance}] partition={msg.partition} offset={msg.offset} "
            f"→ {msg.value['symbol']} ${msg.value['price']}"
        )
except KeyboardInterrupt:
    print(f"[{instance}] stopping.")
finally:
    consumer.close()