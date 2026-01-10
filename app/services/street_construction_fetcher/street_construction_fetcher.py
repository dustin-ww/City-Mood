from datetime import datetime, timezone
from typing import Dict, Any, List
import requests

from common.base_fetcher import BaseFetcher
from common.common_utils import (
    logger,
    get_kafka_producer,
    get_fetch_interval,
    get_last_timestamp,
    set_last_timestamp,
)

# CONFIG
STREET_CONSTRUCTION_API_URL = (
    "https://api.hamburg.de/datasets/v1/baustellen/"
    "collections/baustelle/items"
    "?limit=1000&offset=0&f=json"
)

KAFKA_TOPIC = "hh-street-construction"
REDIS_LAST_FETCH_KEY = "street-construction:last_fetch"


class StreetConstructionFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("Street construction fetch job started")
        self.process_street_constructions()
        logger.info("Street construction fetch job completed")

    def fetch_data(self) -> Dict[str, Any]:
        logger.info("Fetching street construction data from Hamburg API …")
        resp = requests.get(STREET_CONSTRUCTION_API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("type") != "FeatureCollection":
            raise ValueError("Unexpected response format (not FeatureCollection)")

        features = data.get("features", [])
        logger.info(f"Fetched {len(features)} street construction features")

        return data

    def send_features_to_kafka(self, features: List[Dict[str, Any]]):
        producer = get_kafka_producer()
        fetch_ts = datetime.utcnow().isoformat() + "Z"
        sent = 0

        for feature in features:
            try:
                props = feature.get("properties", {})
                geom = feature.get("geometry", {})

                event = {
                    "fetch_timestamp": fetch_ts,
                    "source": "hh_baustellen",
                    "type": "street_construction",
                    "feature_id": feature.get("id"),
                    "geometry": geom,
                    "geometry_type": geom.get("type"),
                    "coordinates": geom.get("coordinates"),
                    "properties": {
                        "titel": props.get("titel"),
                        "organisation": props.get("organisation"),
                        "anlass": props.get("anlass"),
                        "umfang": props.get("umfang"),
                        "baubeginn": props.get("baubeginn"),
                        "bauende": props.get("bauende"),
                        "letzteaktualisierung": props.get("letzteaktualisierung"),
                        "iststoerung": props.get("iststoerung"),
                        "isthotspot": props.get("isthotspot"),
                        "istfreigegeben": props.get("istfreigegeben"),
                        "istoepnveingeschraenkt": props.get("istoepnveingeschraenkt"),
                        "istparkraumeingeschraenkt": props.get("istparkraumeingeschraenkt"),
                        "hatinternetlink": props.get("hatinternetlink"),
                        "internetlink": props.get("internetlink"),
                    },
                }

                producer.send(KAFKA_TOPIC, value=event)
                sent += 1

            except Exception as e:
                logger.error(f"Failed to send feature {feature.get('id')}: {e}")

        producer.flush()
        logger.info(f"Sent {sent} street construction events to Kafka")

    def process_street_constructions(self):
        now = datetime.now(timezone.utc)
        interval = get_fetch_interval()  # Sekunden
        last_fetch = get_last_timestamp(REDIS_LAST_FETCH_KEY)

        if last_fetch:
            if last_fetch.tzinfo is None:
                last_fetch = last_fetch.replace(tzinfo=timezone.utc)
            elapsed = (now - last_fetch).total_seconds()
            if elapsed < interval:
                remaining = interval - elapsed
                logger.info(
                    f"Skipping street construction fetch – next run in "
                    f"{remaining / 3600:.2f}h"
                )
                return

        try:
            data = self.fetch_data()
            features = data.get("features", [])

            if not features:
                logger.warning("No street construction features found")
                return

            self.send_features_to_kafka(features)
            set_last_timestamp(REDIS_LAST_FETCH_KEY, now)

            logger.info("Street construction fetch cycle completed")

        except Exception as e:
            logger.error(f"Street construction fetch failed: {e}")


if __name__ == "__main__":
    fetcher = StreetConstructionFetcher(
        wakeup_topic="fetch-street-construction",
        group_id="street-construction-fetcher",
    )
    fetcher.run()
