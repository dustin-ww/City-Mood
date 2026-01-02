import os
import json
import logging
import feedparser
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RSS = "hh-ndr-news"

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# RSS Parsing
def fetch_rss_feed(url: str) -> feedparser.FeedParserDict:
    logger.info(f"Fetching RSS feed from {url}")
    feed = feedparser.parse(url)
    if feed.bozo:
        logger.error(f"Error parsing RSS feed: {feed.bozo_exception}")
    else:
        logger.info(f"Fetched {len(feed.entries)} RSS entries")
    return feed

def build_rss_event(entry: dict) -> dict:
    event = {
        "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
        "title": entry.get("title"),
        "published": entry.get("published"),
        "summary": entry.get("summary"),
        "id": entry.get("id", entry.get("link"))
    }
    return event

def send_rss_to_kafka(entry: dict):
    event = build_rss_event(entry)
    logger.info(f"Sending RSS event to Kafka: {event['title']}")
    producer.send(TOPIC_RSS, value=event)
    producer.flush()
    logger.info("RSS event sent.")

def process_rss_feed(url: str):
    try:
        feed = fetch_rss_feed(url)
        for entry in feed.entries:
            send_rss_to_kafka(entry)
        logger.info("RSS feed processing completed.")
    except Exception as e:
        logger.error(f"Error during RSS feed processing: {e}")


def main():
    rss_feed_url = "https://www.ndr.de/nachrichten/hamburg/index~rss2.xml"  # RSS-Feed hier einfügen
    logger.info("RSS Kafka Fetcher started – waiting for fetch triggers")

    """ consumer = KafkaConsumer(
        "fetch-rss-feed",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="rss-fetcher",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest"
    )

    for message in consumer:
        event = message.value
        logger.info(f"Received fetch trigger: {event}")
        process_rss_feed(rss_feed_url)
 """
    process_rss_feed(rss_feed_url)

if __name__ == "__main__":
    main()
