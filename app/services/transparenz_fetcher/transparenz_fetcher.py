import os
import json
import time
import schedule
import requests
from kafka import KafkaProducer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = 'hh-transparenz-events'
API_URL = "http://suche.transparenz.hamburg.de/api/3/action/package_search"
KEYWORDS = ["unfall", "störung", "sperrung", "feuerwehr", "polizei", "baustelle"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_transparenz_data():
    logger.info("Fetching Transparenzportal data...")
    
    for kw in KEYWORDS:
        try:
            # Suche nach Keyword, sortiert nach Datum (desc)
            params = {
                "q": kw,
                "rows": 20,
                "sort": "metadata_modified desc"
            }
            resp = requests.get(API_URL, params=params, timeout=10)
            data = resp.json()
            
            if not data.get("success"):
                logger.error(f"API Error for keyword {kw}")
                continue
                
            results = data.get("result", {}).get("results", [])
            logger.info(f"Keyword '{kw}': found {len(results)} items")
            
            for item in results:
                event = {
                    "source": "transparenz_portal",
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                    "event_id": item.get("id"),
                    "title": item.get("title"),
                    "published_at": item.get("metadata_modified"), # oder publishing_date
                    "text": item.get("notes"),
                    "url": item.get("url"), # oder res_url liste
                    "category": [kw],  # Wir nutzen das Suchwort als Kategorie
                    "groups": [g.get("name") for g in item.get("groups", [])]
                }
                producer.send(KAFKA_TOPIC, value=event)
                
        except Exception as e:
            logger.error(f"Error fetching keyword {kw}: {e}")
            
    producer.flush()
    logger.info("Transparenz fetch cycle completed.")

def main():

    logger.info("Traffic Data Fetcher started – waiting for Kafka events")

    Path(INPUT_DIR).mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        "fetch-transparenz",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="transparenz-fetcher",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest"
    )

    for message in consumer:
        event = message.value
        logger.info(f"Received fetch trigger: {event}")
        
        logger.info("Transparenz Fetcher started")
        fetch_transparenz_data()
    

if __name__ == "__main__":
    main()