import os
import json
import time
import schedule
import zipfile
import io
from kafka import KafkaProducer
from datetime import datetime
import logging
from typing import List, Dict, Any
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = 'hh-traffic-data'
INPUT_DIR = './../../data/traffic_hh'

# Init Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def load_local_geojson(file_path: str) -> List[Dict[str, Any]]:
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
        
    except zipfile.BadZipFile as e:
        logger.error(f"Invalid ZIP file: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
        return []
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return []
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return []


def send_features_to_kafka(features: List[Dict[str, Any]], source_file: str):
    """
    Sends individual GeoJSON features to Kafka
    
    Args:
        features: List of GeoJSON features
        source_file: Source file of the data
    """
    fetch_timestamp = datetime.now().isoformat()
    
    for idx, feature in enumerate(features):
        try:
            # Add metadata to feature
            message = {
                'fetch_timestamp': fetch_timestamp,
                'source': source_file,
                'feature_index': idx,
                'feature': feature
            }
            
            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=message)
            
            if (idx + 1) % 100 == 0:
                logger.info(f"{idx + 1}/{len(features)} features sent")
                
        except Exception as e:
            logger.error(f"Error sending feature {idx}: {e}")
    
    producer.flush()
    logger.info(f"All {len(features)} features sent to Kafka")


def fetch_traffic_data():
    """
    Loads traffic data from local ZIP files and sends them to Kafka
    """
    try:
        logger.info(f"Starting traffic data fetch at {datetime.now()}")
        
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
            logger.info(f"Processing file: {zip_file}")
            
            features = load_local_geojson(str(zip_file))
            
            if features:
                send_features_to_kafka(features, str(zip_file))
                total_features += len(features)
            else:
                logger.warning(f"No features received from {zip_file}")
        
        logger.info(f"Traffic data fetch completed. Total: {total_features} features")
        
    except Exception as e:
        logger.error(f"Error during data fetch: {e}")


def main():
    logger.info("Traffic Data Fetcher started")
    
    Path(INPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    fetch_traffic_data()
    
    # Schedule for twice daily (e.g., 6:00 and 18:00)
    schedule.every().day.at("06:00").do(fetch_traffic_data)
    schedule.every().day.at("18:00").do(fetch_traffic_data)
    
    # Optional: Also hourly for real-time traffic data
    # schedule.every().hour.do(fetch_traffic_data)
    
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()