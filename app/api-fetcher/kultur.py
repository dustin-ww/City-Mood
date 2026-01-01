import os
import json
import time
import schedule
import feedparser
from kafka import KafkaProducer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = 'hh-kultur-events'
# Beispiel-Feed: Alle Veranstaltungen in Hamburg
RSS_FEED_URL = "https://www.kultur-hamburg.de/rss/veranstaltungen_alle.xml"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_kultur_events():
    logger.info("Fetching Kultur RSS Feed...")
    feed = feedparser.parse(RSS_FEED_URL)
    
    if feed.bozo:
        logger.error(f"Error parsing RSS: {feed.bozo_exception}")
        return

    logger.info(f"Found {len(feed.entries)} entries.")
    
    count = 0
    for entry in feed.entries:
        # Minimales Event-Schema
        event = {
            "source": "kultur_hamburg",
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "event_id": entry.get("link", ""),
            "title": entry.get("title", ""),
            "published_at": entry.get("published", ""),
            "text": entry.get("summary", ""),
            "url": entry.get("link", ""),
            # Kategorie/Tags oft in 'tags' oder 'category'
            "category": [t.term for t in entry.get("tags", [])] if "tags" in entry else []
        }
        
        try:
            producer.send(KAFKA_TOPIC, value=event)
            count += 1
        except Exception as e:
            logger.error(f"Error sending event: {e}")
            
    producer.flush()
    logger.info(f"Sent {count} Kultur events to Kafka.")

def main():
    logger.info("Kultur Fetcher started")
    fetch_kultur_events()
    
    # Alle 6 Stunden aktualisieren
    schedule.every(6).hours.do(fetch_kultur_events)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()