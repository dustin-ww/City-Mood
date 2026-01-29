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

# Fetch trigger topics
FETCH_TOPICS = [
    "fetch-weather",
    "fetch-traffic",
    "fetch-air-pollution",
    "fetch-bbc-rss",
    "fetch-nyt-rss",
    "fetch-public-alerts",
    "fetch-street-construction",
    "fetch-transparenz",
    "fetch-water-levels",
]

# Data topics (where fetchers write their data)
DATA_TOPICS = [
    "bbc-europe-news",
    "nyt-europe-news",
    "nyt-world-news",
    "hh-air-pollution-current",
    "hh-weather-current",
    "hh-weather-daily",
    "hh-traffic-data",
    "hh-public-alerts-current",
    "hh-street-construction",
    "hh-water-level-current",
    "hh-transparenz-events",
]

ALL_TOPICS = FETCH_TOPICS + DATA_TOPICS

RETENTION_MS = 24 * 60 * 60 * 1000  # 24 hours
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


def ensure_topics_exist():
    logger.info("=" * 80)
    logger.info("KAFKA TOPIC INITIALIZATION")
    logger.info("=" * 80)
    
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    
    try:
        existing_topics = admin_client.list_topics()
        logger.info(f"Found {len(existing_topics)} existing topics")
        
        topics_to_create = []
        topics_already_exist = []
        
        for topic in ALL_TOPICS:
            if topic not in existing_topics:
                topics_to_create.append(topic)
            else:
                topics_already_exist.append(topic)
        
        if topics_already_exist:
            logger.info(f"Topics already exist ({len(topics_already_exist)}):")
            for topic in topics_already_exist:
                logger.info(f"{topic}")
        
        # Create missing topics
        if topics_to_create:
            logger.info(f"Creating missing topics ({len(topics_to_create)}):")
            
            new_topics = []
            for topic in topics_to_create:
                logger.info(f"  → {topic} (retention: {RETENTION_MS}ms)")
                new_topics.append(
                    NewTopic(
                        name=topic,
                        num_partitions=1,
                        replication_factor=1,
                        topic_configs={"retention.ms": str(RETENTION_MS)}
                    )
                )
            
            try:
                admin_client.create_topics(new_topics, validate_only=False)
                logger.info(f"Successfully created {len(new_topics)} topics")
            except TopicAlreadyExistsError as e:
                logger.warning(f"Some topics already existed (race condition): {e}")
            except Exception as e:
                logger.error(f"Error creating topics: {e}")
                raise
        else:
            logger.info("All topics already exist - nothing to create")
        
        logger.info("=" * 80)
        logger.info(f"TOTAL TOPICS: {len(ALL_TOPICS)}")
        logger.info(f"  - Fetch topics: {len(FETCH_TOPICS)}")
        logger.info(f"  - Data topics: {len(DATA_TOPICS)}")
        logger.info("=" * 80)
        
    finally:
        admin_client.close()

# Ensure all topics exist before starting
ensure_topics_exist()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

if TRIGGER_ON_START or IMMEDIATE_TRIGGER:
    logger.info("Immediate fetch trigger enabled – sending events now")
    send_fetch_events(producer)

logger.info("Starting hourly fetch scheduler...")
while True:
    sleep_until_next_full_hour()
    send_fetch_events(producer)