import time
import os
import json
from kafka import KafkaProducer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

TOPICS = [
    "fetch-weather",
    "fetch-traffic",
    "fetch-news"
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

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