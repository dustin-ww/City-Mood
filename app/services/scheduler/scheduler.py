import os
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FETCH_TOPICS = [
    "fetch-weather",
    "fetch-air-pollution",
    "fetch-traffic",
    "fetch-news"
]


# 24h retention time for fetch trigger topics
RETENTION_MS = 24 * 60 * 60 * 1000

# Kafka Admin Client to create topics if they dont exist
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

existing_topics = admin_client.list_topics()
for topic in FETCH_TOPICS:
    if topic not in existing_topics:
        try:
            logger.info(f"Topic '{topic}' does not exist. Creating with 24h retention...")
            admin_client.create_topics([
                NewTopic(
                    name=topic,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"retention.ms": str(RETENTION_MS)}
                )
            ])
            logger.info(f"Topic '{topic}' successfully created.")
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{topic}' was just created by another client.")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    timestamp = datetime.now().isoformat()

    for topic in FETCH_TOPICS:
        event = {
            "type": "FETCH_TRIGGER",
            "topic": topic,
            "timestamp": timestamp
        }
        logger.info(f"Sending fetch trigger event to topic: {topic} for time {timestamp}")
        producer.send(topic, event)

    logger.info("Sent fetch trigger events to all topics.")
    producer.flush()

    # Hourly trigger
    time.sleep(3600)