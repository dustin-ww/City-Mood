import os
import json
import logging
import requests

from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_TRIGGER = "fetch-water-levels"
TOPIC_WATER_LEVEL_CURRENT = "hh-water-level-current"

PEGELONLINE_URL = (
    "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    "?includeTimeseries=true"
    "&includeCurrentMeasurement=true"
    "&includeCharacteristicValues=true"
    "&waters=ELBE"
    "&timeseries=W"
    "&latitude=53.551085"
    "&longitude=9.993682"
    "&radius=10"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_water_level_data() -> list[dict]:
    logger.info("Fetching water level data from PegelOnline ...")

    resp = requests.get(PEGELONLINE_URL, timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"PegelOnline returned HTTP {resp.status_code}")

    data = resp.json()
    logger.info(f"Fetched {len(data)} stations.")
    return data


def send_current_water_levels(stations: list[dict]):
    sent_events = 0

    for station in stations:
        for ts in station.get("timeseries", []):
            current = ts.get("currentMeasurement")
            if not current:
                continue

            event = {
                "fetch_timestamp": datetime.utcnow().isoformat(),
                "source": "pegelonline",
                "type": "current_water_level",
                "station": {
                    "uuid": station.get("uuid"),
                    "number": station.get("number"),
                    "shortname": station.get("shortname"),
                    "longname": station.get("longname"),
                    "agency": station.get("agency"),
                    "km": station.get("km"),
                    "longitude": station.get("longitude"),
                    "latitude": station.get("latitude"),
                    "water": station.get("water", {}).get("shortname"),
                },
                "measurement": {
                    "timestamp": current.get("timestamp"),
                    "value_cm": current.get("value"),
                    "unit": ts.get("unit"),
                    "stateMnwMhw": current.get("stateMnwMhw"),
                    "stateNswHsw": current.get("stateNswHsw"),
                }
            }

            logger.info(
                f"Sending water level for {station.get('shortname')} "
                f"({current.get('value')} {ts.get('unit')})"
            )

            producer.send(TOPIC_WATER_LEVEL_CURRENT, value=event)
            sent_events += 1

    producer.flush()
    logger.info(f"Sent {sent_events} water level events.")


def process_water_levels():
    try:
        logger.info(f"Water level fetch job started at {datetime.now()}")
        stations = fetch_water_level_data()
        send_current_water_levels(stations)
        logger.info("Water level fetch cycle completed.")
    except Exception as e:
        logger.error(f"Error during water level processing: {e}")


def main():
    logger.info("Water Level Fetcher started – waiting for Kafka trigger")

    consumer = KafkaConsumer(
        TOPIC_TRIGGER,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="water-level-fetcher",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest"
    )

    for message in consumer:
        logger.info(f"Received fetch trigger: {message.value}")
        process_water_levels()


if __name__ == "__main__":
    main()
