import os
import json
import logging
import feedparser
from datetime import datetime, timedelta
from kafka import KafkaProducer
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# KAFKA CONFIG
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_BOOTSTRAP_SERVERSTOPIC_RSS = "hh-ndr-news"

# REDIS CONFIG
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PROCESSED_IDS = "rss:processed_ids"
REDIS_LAST_FETCH = "rss:last_fetch"
REDIS_DEFAULT_TTL = 30 * 24 * 3600  # 30 Days

# API CONFIG
API_REFRESH_INTERVAL_HOURS = float(os.getenv("API_REFRESH_INTERVAL_HOURS", 3))  
API_REFRESH_INTERVAL = int(API_REFRESH_INTERVAL_HOURS * 3600) 

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Redis Client
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def is_duplicate(entry_id: str) -> bool:
    return r.sismember(REDIS_PROCESSED_IDS, entry_id)

def mark_processed(entry_id: str):
    r.sadd(REDIS_PROCESSED_IDS, entry_id)
    r.expire(REDIS_PROCESSED_IDS, REDIS_DEFAULT_TTL)

def get_last_fetch_time() -> datetime | None:
    ts = r.get(REDIS_LAST_FETCH)
    if ts:
        return datetime.fromisoformat(ts)
    return None

def set_last_fetch_time(ts: datetime):
    r.set(REDIS_LAST_FETCH, ts.isoformat())

def get_fetch_interval() -> int:
    """Fetch interval in seconds, can be dynamically updated via Redis API param"""
    val = r.get("rss:fetch_interval")
    if val:
        try:
            return int(val)
        except ValueError:
            pass
    return REDIS_DEFAULT_FETCH_INTERVAL

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
        "id": entry.get("id", entry.get("guid", entry.get("link")))
    }
    return event

def send_rss_to_kafka(entry: dict):
    entry_id = entry.get("id", entry.get("guid", entry.get("link")))
    if is_duplicate(entry_id):
        logger.info(f"Skipping duplicate RSS entry: {entry.get('title')}")
        return

    event = build_rss_event(entry)
    logger.info(f"Sending RSS event to Kafka: {event['title']}")
    producer.send(TOPIC_RSS, value=event)
    producer.flush()
    logger.info("RSS event sent.")
    mark_processed(entry_id)

def process_rss_feed(url: str):
    # Fetch interval check
    last_fetch = get_last_fetch_time()
    interval = get_fetch_interval()
    now = datetime.utcnow()
    if last_fetch and (now - last_fetch).total_seconds() < interval:
        logger.info(f"Skipping RSS fetch. Only {(now - last_fetch).total_seconds()/3600:.2f}h since last fetch. Required interval: {interval/3600:.2f}h")
        return

    try:
        feed = fetch_rss_feed(url)
        for entry in feed.entries:
            send_rss_to_kafka(entry)
        set_last_fetch_time(now)
        logger.info("RSS feed processing completed.")
    except Exception as e:
        logger.error(f"Error during RSS feed processing: {e}")


def main():
    rss_feed_url = "https://www.ndr.de/nachrichten/hamburg/index~rss2.xml"
    logger.info("RSS Kafka Fetcher started – processing feed")
    process_rss_feed(rss_feed_url)
    logger.info("RSS Kafka Fetcher finished processing")

if __name__ == "__main__":
    main()
