from datetime import datetime
from pathlib import Path
from common.base_fetcher import BaseFetcher
from common.common_utils import logger, get_kafka_producer
import json
import zipfile
from typing import List, Dict, Any

# SERVICE CONFIG
INPUT_DIR = './../../data/traffic_hh'
KAFKA_TOPIC = 'hh-traffic-data'


class TrafficFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("Traffic data fetch job started")
        self.fetch_traffic_data()
        logger.info("Traffic data fetch job completed")

    def load_local_geojson(self, file_path: str) -> List[Dict[str, Any]]:
        """Extract features from a ZIP file containing GeoJSON"""
        try:
            logger.info(f"Loading zip file from: {file_path}")
            features = []

            with zipfile.ZipFile(file_path, 'r') as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.endswith('.geojson') or file_name.endswith('.json'):
                        logger.info(f"Processing file: {file_name}")
                        with zip_file.open(file_name) as json_file:
                            data = json.load(json_file)
                            if data.get('type') == 'FeatureCollection':
                                features.extend(data.get('features', []))
                            elif data.get('type') == 'Feature':
                                features.append(data)

            logger.info(f"{len(features)} features extracted from {file_path}")
            return features

        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return []

    def send_features_to_kafka(self, features: List[Dict[str, Any]], source_file: str):
        """Send individual GeoJSON features to Kafka"""
        producer = get_kafka_producer()
        fetch_timestamp = datetime.utcnow().isoformat()

        for idx, feature in enumerate(features):
            try:
                message = {
                    'fetch_timestamp': fetch_timestamp,
                    'source': source_file,
                    'feature_index': idx,
                    'feature': feature
                }
                producer.send(KAFKA_TOPIC, value=message)
                if (idx + 1) % 100 == 0:
                    logger.info(f"{idx + 1}/{len(features)} features sent")
            except Exception as e:
                logger.error(f"Error sending feature {idx}: {e}")

        producer.flush()
        logger.info(f"All {len(features)} features sent to Kafka")

    def fetch_traffic_data(self):
        """Load traffic data from local ZIP files and send to Kafka"""
        try:
            logger.info(f"Starting traffic data fetch at {datetime.utcnow()}")
            input_path = Path(INPUT_DIR)
            if not input_path.exists():
                logger.error(f"Input directory {INPUT_DIR} does not exist!")
                return

            zip_files = list(input_path.glob('*.zip'))
            if not zip_files:
                logger.warning(f"No ZIP files found in {INPUT_DIR}")
                return

            total_features = 0
            for zip_file in zip_files:
                features = self.load_local_geojson(str(zip_file))
                if features:
                    self.send_features_to_kafka(features, str(zip_file))
                    total_features += len(features)
                else:
                    logger.warning(f"No features found in {zip_file}")

            logger.info(f"Traffic data fetch completed. Total: {total_features} features")

        except Exception as e:
            logger.error(f"Error during traffic data fetch: {e}")


if __name__ == "__main__":
    fetcher = TrafficFetcher(wakeup_topic="fetch-traffic", group_id="traffic-fetcher")
    fetcher.run()
