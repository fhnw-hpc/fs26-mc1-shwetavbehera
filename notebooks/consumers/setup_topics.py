import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from shared.kafka_config import BOOTSTRAP_SERVERS

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