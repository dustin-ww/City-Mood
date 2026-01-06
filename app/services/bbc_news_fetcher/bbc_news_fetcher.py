from datetime import datetime
from common.base_fetcher import BaseFetcher
from common.common_utils import (
    logger,
    get_kafka_producer,
    get_fetch_interval,
    is_duplicate,
    get_last_timestamp,
    set_last_timestamp,
)
import feedparser

from flair.data import Sentence
from flair.nn import Classifier

# Config
BBC_FEED = {
    "name": "europe",
    "rss_url": "https://feeds.bbci.co.uk/news/world/europe/rss.xml",
    "kafka_topic": "bbc-europe-news",
    "redis_processed": "rss:bbc:europe:processed_ids",
    "redis_last_fetch": "rss:bbc:europe:last_fetch",
}


class BbcRssFetcher(BaseFetcher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sentiment_tagger = Classifier.load("sentiment")

    def process_message(self, message: dict):
        logger.info("BBC RSS fetch job started")
        self.process_feed()
        logger.info("BBC RSS fetch job completed")

    def fetch_rss_feed(self):
        logger.info(f"Fetching BBC RSS feed from {BBC_FEED['rss_url']}")
        feed = feedparser.parse(BBC_FEED["rss_url"])

        if feed.bozo:
            logger.error(f"Error parsing BBC RSS feed: {feed.bozo_exception}")
        else:
            logger.info(f"Fetched {len(feed.entries)} BBC RSS entries")

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

    def build_rss_event(self, entry: dict) -> dict:
        sentiment = self.analyze_sentiment(entry.get("title"))

        # optional: Thumbnail
        thumbnail = None
        if "media_thumbnail" in entry:
            media_list = entry.media_thumbnail
            if media_list and isinstance(media_list, list):
                thumbnail = media_list[0].get("url")

        return {
            "source": "bbc",
            "section": BBC_FEED["name"],
            "fetch_timestamp": datetime.utcnow().isoformat() + "Z",

            "id": entry.get("guid"),
            "title": entry.get("title"),
            "url": entry.get("link"),
            "published": entry.get("published") or entry.get("pubDate"),
            "summary": entry.get("description"),
            "categories": [tag["term"] for tag in entry.get("tags", [])] if "tags" in entry else [],
            "headline_sentiment": sentiment,
            "thumbnail": thumbnail,
        }

    def process_feed(self):
        last_fetch = get_last_timestamp(BBC_FEED["redis_last_fetch"])
        interval = get_fetch_interval()
        now = datetime.utcnow()

        if last_fetch and (now - last_fetch).total_seconds() < interval:
            logger.info(
                f"Skipping BBC {BBC_FEED['name']} RSS fetch. "
                f"Only {(now - last_fetch).total_seconds()/3600:.2f}h since last fetch."
            )
            return

        try:
            feed = self.fetch_rss_feed()

            for entry in feed.entries:
                entry_id = entry.get("guid")
                if not entry_id:
                    logger.warning("Skipping entry without GUID")
                    continue

                if is_duplicate(BBC_FEED["redis_processed"], entry_id):
                    logger.info(f"Skipping duplicate BBC entry: {entry.get('title')}")
                    continue

                event = self.build_rss_event(entry)
                producer = get_kafka_producer()
                logger.info(f"Sending BBC RSS event to Kafka: {event['title']}")
                producer.send(BBC_FEED["kafka_topic"], value=event)
                producer.flush()

            set_last_timestamp(BBC_FEED["redis_last_fetch"], now)
            logger.info(f"BBC {BBC_FEED['name']} RSS feed processing completed.")

        except Exception as e:
            logger.error(f"Error during BBC {BBC_FEED['name']} RSS processing: {e}")


if __name__ == "__main__":
    fetcher = BbcRssFetcher(
        wakeup_topic="fetch-bbc-rss",
        group_id="bbc-rss-fetcher"
    )
    fetcher.run()
