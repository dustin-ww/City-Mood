import os
from datetime import datetime
from common.base_fetcher import BaseFetcher
from common.common_utils import logger, get_kafka_producer, get_last_timestamp, set_last_timestamp, get_fetch_interval
import requests
import json

KAFKA_TOPIC = 'hh-transparenz-events'
API_URL = "http://suche.transparenz.hamburg.de/api/3/action/package_search"
KEYWORDS = ["unfall", "störung", "sperrung", "feuerwehr", "polizei", "baustelle"]
REDIS_LAST_FETCH_KEY = "transparenz:last_fetch"


class TransparenzFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("Transparenz fetch job started")
        self.process_transparenz_data()
        logger.info("Transparenz fetch job completed")

    def fetch_transparenz_data(self):
        producer = get_kafka_producer()
        for kw in KEYWORDS:
            try:
                params = {
                    "q": kw,
                    "rows": 20,
                    "sort": "metadata_modified desc"
                }
                resp = requests.get(API_URL, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                if not data.get("success"):
                    logger.error(f"API Error for keyword {kw}")
                    continue

                results = data.get("result", {}).get("results", [])
                logger.info(f"Keyword '{kw}': found {len(results)} items")

                for item in results:
                    event = {
                        "source": "transparenz_portal",
                        "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
                        "event_id": item.get("id"),
                        "title": item.get("title"),
                        "published_at": item.get("metadata_modified"),
                        "text": item.get("notes"),
                        "url": item.get("url"),
                        "category": [kw],
                        "groups": [g.get("name") for g in item.get("groups", [])]
                    }
                    producer.send(KAFKA_TOPIC, value=event)

            except Exception as e:
                logger.error(f"Error fetching keyword {kw}: {e}")

        producer.flush()
        logger.info("All Transparenz events sent.")

    def process_transparenz_data(self):
        now = datetime.now(datetime.timezone.utc)
        last_fetch = get_last_timestamp(REDIS_LAST_FETCH_KEY)
        interval = get_fetch_interval()  # Sekunden

        if last_fetch and (now - last_fetch).total_seconds() < interval:
            remaining = interval - (now - last_fetch).total_seconds()
            logger.info(f"Skipping Transparenz fetch – next run in {remaining/3600:.2f}h")
            return

        try:
            self.fetch_transparenz_data()
            set_last_timestamp(REDIS_LAST_FETCH_KEY, now)
            logger.info("Transparenz fetch cycle completed successfully")
        except Exception as e:
            logger.error(f"Transparenz fetch failed: {e}")


if __name__ == "__main__":
    fetcher = TransparenzFetcher(
        wakeup_topic="fetch-transparenz",
        group_id="transparenz-fetcher"
    )
    fetcher.run()
