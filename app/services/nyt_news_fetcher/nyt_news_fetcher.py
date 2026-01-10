from datetime import datetime, timezone
from common.base_fetcher import BaseFetcher
from common.common_utils import (
    logger,
    get_kafka_producer,
    get_fetch_interval,
    get_last_timestamp,
    set_last_timestamp,
)
import feedparser
from dateutil import parser as dateparser
from flair.data import Sentence
from flair.nn import Classifier

NYT_FEEDS = [
    {
        "name": "europe",
        "rss_url": "https://rss.nytimes.com/services/xml/rss/nyt/Europe.xml",
        "kafka_topic": "nyt-europe-news",
        "redis_last_fetch": "rss:nyt:europe:last_fetch",
    },
    {
        "name": "world",
        "rss_url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "kafka_topic": "nyt-world-news",
        "redis_last_fetch": "rss:nyt:world:last_fetch",
    },
]

class RssFetcher(BaseFetcher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sentiment_tagger = Classifier.load("sentiment")

    def process_message(self, message: dict):
        logger.info("NYT RSS fetch job started")
        for feed_cfg in NYT_FEEDS:
            self.process_single_feed(feed_cfg)
        logger.info("NYT RSS fetch job completed")

    def fetch_rss_feed(self, rss_url: str):
        logger.info(f"Fetching NYT RSS feed from {rss_url}")
        feed = feedparser.parse(rss_url)

        if feed.bozo:
            logger.error(f"Error parsing NYT RSS feed: {feed.bozo_exception}")
        else:
            logger.info(f"Fetched {len(feed.entries)} RSS entries")

        return feed

    def analyze_sentiment(self, text: str) -> dict | None:
        if not text:
            return None
        sentence = Sentence(text)
        self.sentiment_tagger.predict(sentence)
        if not sentence.labels:
            return None
        label = sentence.labels[0]
        return {
            "label": label.value,
            "score": round(label.score, 4)
        }

    def parse_published(self, entry) -> str | None:
        """Versuche Published-Zeit zu parsen, sonst None"""
        try:
            pub = entry.get("published") or entry.get("pubDate")
            if not pub:
                return None
            dt = dateparser.parse(pub)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return None

    def build_rss_event(self, entry: dict, section: str) -> dict:
        sentiment = self.analyze_sentiment(entry.get("title"))
        event_time = self.parse_published(entry)

        return {
            "source": "nytimes",
            "section": section,
            "event_time": event_time,
            "ingest_time": datetime.utcnow().isoformat() + "Z",
            "id": entry.get("guid"),
            "title": entry.get("title"),
            "url": entry.get("link"),
            "published": entry.get("published"),
            "summary": entry.get("description"),
            "authors": entry.get("authors", []),
            "categories": [tag["term"] for tag in entry.get("tags", [])] if "tags" in entry else [],
            "headline_sentiment": sentiment,
        }

    def process_single_feed(self, feed_cfg: dict):
        now = datetime.now(timezone.utc)
        last_fetch = get_last_timestamp(feed_cfg["redis_last_fetch"])
        interval = get_fetch_interval()

        # Zeitliche Sperre prüfen
        if last_fetch:
            elapsed = (now - last_fetch).total_seconds()
            if elapsed < interval:
                logger.info(
                    f"Skipping NYT {feed_cfg['name']} RSS fetch. "
                    f"{elapsed/3600:.2f}h elapsed, "
                    f"required {interval/3600:.2f}h"
                )
                return

        try:
            feed = self.fetch_rss_feed(feed_cfg["rss_url"])
            producer = get_kafka_producer()
            seen_ids = set()  # Nur innerhalb dieses Fetches

            for entry in feed.entries:
                entry_id = entry.get("guid")
                if not entry_id:
                    logger.warning("Skipping entry without GUID")
                    continue

                if entry_id in seen_ids:
                    logger.debug(f"Duplicate entry within fetch skipped: {entry.get('title')}")
                    continue
                seen_ids.add(entry_id)

                event = self.build_rss_event(entry, feed_cfg["name"])
                logger.info(f"Sending NYT {feed_cfg['name']} RSS event to Kafka: {event['title']}")
                producer.send(feed_cfg["kafka_topic"], value=event)

            producer.flush()
            set_last_timestamp(feed_cfg["redis_last_fetch"], now)
            logger.info(f"NYT {feed_cfg['name']} RSS feed processing completed.")

        except Exception as e:
            logger.error(f"Error during NYT {feed_cfg['name']} RSS processing: {e}")


if __name__ == "__main__":
    fetcher = RssFetcher(
        wakeup_topic="fetch-nyt-rss",
        group_id="nyt-rss-fetcher"
    )
    fetcher.run()
