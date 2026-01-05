import os
import json
import time
import schedule
import requests
from kafka import KafkaProducer, KafkaConsumer

from datetime import datetime
import logging
from pathlib import Path


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_CURRENT = "hh-air-pollution-current" 
TOPIC_DAILY = "hh-air-pollution-daily"       
OPEN_METEO_URL = (
    "https://air-quality-api.open-meteo.com/v1/air-quality?"
    "latitude=53.5507&longitude=9.993&"
    "hourly=pm10,pm2_5,carbon_monoxide,carbon_dioxide,sulphur_dioxide,"
    "ozone,methane,ammonia,aerosol_optical_depth,dust,alder_pollen,mugwort_pollen,"
    "grass_pollen,birch_pollen,olive_pollen,ragweed_pollen&"
    "current=european_aqi,aerosol_optical_depth,ammonia,dust,alder_pollen,"
    "birch_pollen,grass_pollen,mugwort_pollen,olive_pollen,ragweed_pollen,pm2_5,pm10,"
    "nitrogen_dioxide,carbon_monoxide,sulphur_dioxide,ozone&forecast_days=1"
)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_air_pollution_data() -> dict:
    logger.info("Fetching air pollution data from Open-Meteo ...")

    resp = requests.get(OPEN_METEO_URL, timeout=15)

    if resp.status_code != 200:
        raise RuntimeError(f"Open-Meteo returned HTTP {resp.status_code}")

    data = resp.json()
    logger.info("Air pollution data fetched successfully.")
    return data


def send_current_air_pollution(data: dict):
    """
    Sends only the **current** air pollution entry into Kafka.
    """
    event = {
        "fetch_timestamp": datetime.utcnow().isoformat(),
        "source": "open-meteo",
        "type": "current_air_pollution",
        "timezone": data.get("timezone"),
        "location": {
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "elevation": data.get("elevation")
        },
        "current": data.get("current")
    }

    logger.info("Sending current air pollution event to Kafka ...")
    producer.send(TOPIC_CURRENT, value=event)
    producer.flush()
    logger.info("Current air pollution sent.")

def send_daily_air_pollution(data: dict):
    """
    Sends the daily air pollution block as a **single event** to Kafka.
    """
    event = {
        "fetch_timestamp": datetime.utcnow().isoformat(),
        "source": "open-meteo",
        "type": "daily_air_pollution",
        "timezone": data.get("timezone"),
        "location": {
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "elevation": data.get("elevation")
        },
        "daily": data.get("daily")
    }

    logger.info("Sending daily air pollution event to Kafka ...")
    producer.send(TOPIC_DAILY, value=event)
    producer.flush()
    logger.info("Daily air pollution sent.")

def process_air_pollution():
    try:
        logger.info(f"Air pollution fetch job started at {datetime.now()}")
        data = fetch_air_pollution_data()

        send_current_air_pollution(data)
        send_daily_air_pollution(data)

        logger.info("Air pollution fetch cycle completed.")

    except Exception as e:
        logger.error(f"Error during air pollution processing: {e}")

def main():
    logger.info("Air Pollution Fetcher started – waiting for Kafka events")

    consumer = KafkaConsumer(
        "fetch-air-pollution",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="air-pollution-fetcher",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest"
    )

    for message in consumer:
        event = message.value
        logger.info(f"Received fetch trigger: {event}")

        process_air_pollution()

if __name__ == "__main__":
    main()
