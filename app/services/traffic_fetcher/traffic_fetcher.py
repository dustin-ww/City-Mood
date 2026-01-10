import os
from datetime import datetime, timezone
from typing import Dict, Any, List
import io
import json
import zipfile
import requests

from common.base_fetcher import BaseFetcher
from common.common_utils import (
    logger,
    get_kafka_producer,
    get_last_timestamp,
    set_last_timestamp,
    get_fetch_interval,
)

# SERVICE CONFIG
TRAFFIC_ZIP_URL = (
    "https://geodienste.hamburg.de/download"
    "?url=https://geodienste.hamburg.de/wfs_hh_verkehrslage"
    "&f=json"
)
KAFKA_TOPIC = "hh-traffic-data"
REDIS_LAST_FETCH_KEY = "traffic:last_fetch"

class TrafficFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("Traffic fetch job started")
        self.process_traffic_data()
        logger.info("Traffic fetch job completed")

    def fetch_zip(self) -> bytes:
        logger.info("Downloading traffic ZIP file from Hamburg GeoServices …")
        resp = requests.get(TRAFFIC_ZIP_URL, timeout=60)
        resp.raise_for_status()
        logger.info(f"Downloaded {len(resp.content)/1024:.1f} KB ZIP archive")
        return resp.content

    def extract_features_from_zip(self, zip_bytes: bytes) -> List[Dict[str, Any]]:
        features: List[Dict[str, Any]] = []
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            json_files = [name for name in zf.namelist() if name.endswith(".json") or name.endswith(".geojson")]
            logger.info(f"Found {len(json_files)} JSON files in ZIP")
            for file_name in json_files:
                logger.info(f"Parsing {file_name}")
                with zf.open(file_name) as f:
                    data = json.load(f)
                    if data.get("type") != "FeatureCollection":
                        logger.warning(f"Skipping {file_name} (not a FeatureCollection)")
                        continue
                    features.extend(data.get("features", []))
        logger.info(f"Extracted total {len(features)} traffic features")
        return features

    def send_features_to_kafka(self, features: List[Dict[str, Any]]):
        producer = get_kafka_producer()
        fetch_timestamp = datetime.utcnow().isoformat() + "Z"
        sent = 0
        for idx, feature in enumerate(features):
            try:
                event = {
                    "fetch_timestamp": fetch_timestamp,
                    "source": "hh_verkehrslage",
                    "type": "traffic_state",
                    "feature_id": feature.get("id"),
                    "properties": feature.get("properties", {}),
                    "srsName": feature.get("srsName"),
                }
                producer.send(KAFKA_TOPIC, value=event)
                sent += 1
                if sent % 500 == 0:
                    logger.info(f"{sent}/{len(features)} features sent")
            except Exception as e:
                logger.error(f"Kafka send failed for feature {idx}: {e}")
        producer.flush()
        logger.info(f"Sent {sent} traffic events to Kafka")

    def process_traffic_data(self):
        last_fetch = get_last_timestamp(REDIS_LAST_FETCH_KEY)
        now = datetime.utcnow()
        interval = get_fetch_interval()  # in Sekunden

        if last_fetch and (now - last_fetch).total_seconds() < interval:
            remaining = interval - (now - last_fetch).total_seconds()
            logger.info(f"Skipping traffic fetch – next run in {remaining/3600:.2f}h")
            return

        try:
            zip_bytes = self.fetch_zip()
            features = self.extract_features_from_zip(zip_bytes)
            if not features:
                logger.warning("No traffic features extracted")
                return

            self.send_features_to_kafka(features)
            set_last_timestamp(REDIS_LAST_FETCH_KEY, now)
            logger.info("Traffic fetch cycle completed successfully")

        except Exception as e:
            logger.error(f"Traffic fetch failed: {e}")


if __name__ == "__main__":
    fetcher = TrafficFetcher(
        wakeup_topic="fetch-traffic",
        group_id="traffic-fetcher",
    )
    fetcher.run()
