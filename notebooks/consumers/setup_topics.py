"""
Part 2 - Topic Setup
Creates the experimental topics with different partition/replication configs.

Run once before starting Part 2 experiments:
   docker exec -it jupyter1 python consumers/setup_topics.py

Then check Kafdrop at http://localhost:9000 to inspect the topics.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"

TOPICS_TO_CREATE = [
    # (name,               partitions, replication_factor)
    ("stock-ticks-p1-r1",  1,          1),
    ("stock-ticks-p3-r1",  3,          1),
    ("stock-ticks-p3-r3",  3,          3),
]

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

for name, partitions, replication in TOPICS_TO_CREATE:
    try:
        admin.create_topics([NewTopic(
            name=name,
            num_partitions=partitions,
            replication_factor=replication,
        )])
        print(f"Created topic '{name}' · partitions={partitions} · replication={replication}")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists, skipping.")

admin.close()
print("Done. Check Kafdrop at http://localhost:9000 to inspect the topics.")