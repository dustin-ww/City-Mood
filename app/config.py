import os

# config.py
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

FETCH_TOPICS = [
    "fetch-weather",
    "fetch-air-pollution",
    "fetch-traffic",
    "fetch-news"
]
