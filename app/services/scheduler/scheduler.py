import os
import json
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

FETCH_TOPICS = [
    #"fetch-weather",
    "fetch-traffic",
    # "fetch-air-pollution",
    # "fetch-traffic",
    # "fetch-news"
]

RETENTION_MS = 24 * 60 * 60 * 1000

IMMEDIATE_TRIGGER = os.getenv("SCHEDULER_IMMEDIATE_TRIGGER", "true").lower() == "true"
TRIGGER_ON_START = os.getenv("SCHEDULER_TRIGGER_ON_START", "false").lower() == "true"


def sleep_until_next_full_hour():
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    sleep_seconds = (next_hour - now).total_seconds()
    logger.info(f"Sleeping {int(sleep_seconds)}s until next full hour ({next_hour.isoformat()})")
    time.sleep(sleep_seconds)


def send_fetch_events(producer):
    timestamp = datetime.now().isoformat()
    for topic in FETCH_TOPICS:
        event = {
            "type": "FETCH_TRIGGER",
            "topic": topic,
            "timestamp": timestamp
        }
        logger.info(f"Sending fetch trigger to {topic} at {timestamp}")
        producer.send(topic, event)
    producer.flush()


# setup/create topics if they dont exist
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
existing_topics = admin_client.list_topics()

for topic in FETCH_TOPICS:
    if topic not in existing_topics:
        try:
            logger.info(f"Creating topic '{topic}' with configured retention of {RETENTION_MS}ms")
            admin_client.create_topics([
                NewTopic(
                    name=topic,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"retention.ms": str(RETENTION_MS)}
                )
            ])
        except TopicAlreadyExistsError:
            pass

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# immediate trigger (for dev)
if TRIGGER_ON_START or IMMEDIATE_TRIGGER:
    logger.info("Immediate fetch trigger enabled – sending events now")
    send_fetch_events(producer)

while True:
    sleep_until_next_full_hour()
    send_fetch_events(producer)
