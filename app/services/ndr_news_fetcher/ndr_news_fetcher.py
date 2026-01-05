from datetime import datetime
from common.base_fetcher import BaseFetcher
from common.common_utils import logger, get_kafka_producer, get_redis_client, get_fetch_interval, is_duplicate, mark_processed, get_last_timestamp, set_last_timestamp
import feedparser

# SERVICE SPECIFIC CONFIG
RSS_FEED_URL = "https://www.ndr.de/nachrichten/hamburg/index~rss2.xml"
TOPIC_RSS = "hh-ndr-news"
REDIS_PROCESSED_IDS = "rss:processed_ids"
REDIS_LAST_FETCH = "rss:last_fetch"


class RssFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("RSS fetch job started")
        self.process_rss_feed()
        logger.info("RSS fetch job completed")

    def fetch_rss_feed(self):
        logger.info(f"Fetching RSS feed from {RSS_FEED_URL}")
        feed = feedparser.parse(RSS_FEED_URL)
        if feed.bozo:
            logger.error(f"Error parsing RSS feed: {feed.bozo_exception}")
        else:
            logger.info(f"Fetched {len(feed.entries)} RSS entries")
        return feed

    def build_rss_event(self, entry: dict) -> dict:
        return {
            "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
            "title": entry.get("title"),
            "published": entry.get("published"),
            "summary": entry.get("summary"),
            "id": entry.get("id", entry.get("guid", entry.get("link")))
        }

    def send_rss_to_kafka(self, entry: dict):
        entry_id = entry.get("id", entry.get("guid", entry.get("link")))

        # Deduplication
        if is_duplicate(REDIS_PROCESSED_IDS, entry_id):
            logger.info(f"Skipping duplicate RSS entry: {entry.get('title')}")
            return

        event = self.build_rss_event(entry)
        producer = get_kafka_producer()
        logger.info(f"Sending RSS event to Kafka: {event['title']}")
        producer.send(TOPIC_RSS, value=event)
        producer.flush()
        #mark_processed(REDIS_PROCESSED_IDS, entry_id, REDIS_DEFAULT_TTL)
        logger.info("RSS event sent.")

    def process_rss_feed(self):
        # Fetch interval check
        last_fetch = get_last_timestamp(REDIS_LAST_FETCH)
        interval = get_fetch_interval()
        now = datetime.utcnow()
        if last_fetch and (now - last_fetch).total_seconds() < interval:
            logger.info(
                f"Skipping RSS fetch. Only {(now - last_fetch).total_seconds()/3600:.2f}h since last fetch. "
                f"Required interval: {interval/3600:.2f}h"
            )
            return

        try:
            feed = self.fetch_rss_feed()
            for entry in feed.entries:
                self.send_rss_to_kafka(entry)
            set_last_timestamp(REDIS_LAST_FETCH, now)
            logger.info("RSS feed processing completed.")
        except Exception as e:
            logger.error(f"Error during RSS feed processing: {e}")


if __name__ == "__main__":
    fetcher = RssFetcher(wakeup_topic="fetch-ndr-rss-feed", group_id="ndr-rss-fetcher")
    fetcher.run()
