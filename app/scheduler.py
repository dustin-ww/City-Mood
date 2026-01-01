import time
import os
import json
from kafka import KafkaProducer
from datetime import datetime

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
    timestamp = datetime.now(time.timezone.utc).isoformat()

    for topic in TOPICS:
        event = {
            "type": "FETCH_TRIGGER",
            "topic": topic,
            "timestamp": timestamp
        }
        producer.send(topic, event)

    producer.flush()
    time.sleep(3600)