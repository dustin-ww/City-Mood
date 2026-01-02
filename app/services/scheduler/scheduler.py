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

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

TOPICS = [
    "fetch-weather",
    "fetch-air-pollution",
    "fetch-traffic",
    "fetch-news"
]

# --- Kafka Admin zum Prüfen/Erstellen der Topics ---
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

existing_topics = admin_client.list_topics()
for topic in TOPICS:
    if topic not in existing_topics:
        try:
            logger.info(f"Topic '{topic}' existiert nicht. Erstelle...")
            admin_client.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
            logger.info(f"Topic '{topic}' erfolgreich erstellt.")
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{topic}' wurde gerade erstellt von einem anderen Client.")

# --- Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --- Endlosschleife zum Senden ---
while True:
    timestamp = datetime.now().isoformat()

    for topic in TOPICS:
        event = {
            "type": "FETCH_TRIGGER",
            "topic": topic,
            "timestamp": timestamp
        }
        logger.info(f"Sending fetch trigger event to topic: {topic} for time {timestamp}")
        producer.send(topic, event)

    logger.info("Sent fetch trigger events to all topics.")
    producer.flush()
    time.sleep(3600)
