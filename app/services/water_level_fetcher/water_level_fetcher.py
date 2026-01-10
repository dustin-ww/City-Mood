import os
from datetime import datetime
from common.base_fetcher import BaseFetcher
from common.common_utils import logger, get_kafka_producer, get_fetch_interval, get_last_timestamp, set_last_timestamp
import requests

# =========================
# SERVICE CONFIG
# =========================
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
CURRENT_WATER_LEVEL_TOPIC = "hh-water-level-current"
REDIS_LAST_FETCH_KEY = "water-level:last_fetch"

class WaterLevelFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("Water level fetch job started")
        self.process_water_level_data()
        logger.info("Water level fetch job completed")

    def process_water_level_data(self):
        now = datetime.utcnow()
        last_fetch = get_last_timestamp(REDIS_LAST_FETCH_KEY)
        interval = get_fetch_interval()

        if last_fetch and (now - last_fetch).total_seconds() < interval:
            remaining = interval - (now - last_fetch).total_seconds()
            logger.info(f"Skipping water level fetch – next run in {remaining/3600:.2f}h")
            return

        try:
            stations = self.fetch_water_level_data()
            self.send_current_water_levels(stations)
            set_last_timestamp(REDIS_LAST_FETCH_KEY, now)
            logger.info("Water level fetch cycle completed successfully")
        except Exception as e:
            logger.error(f"Error during water level fetch: {e}")

    def fetch_water_level_data(self) -> list[dict]:
        logger.info("Fetching water level data from PegelOnline ...")
        resp = requests.get(PEGELONLINE_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"Fetched {len(data)} stations.")
        return data

    def send_current_water_levels(self, stations: list[dict]):
        producer = get_kafka_producer()
        sent_events = 0

        for station in stations:
            for ts in station.get("timeseries", []):
                current = ts.get("currentMeasurement")
                if not current:
                    continue

                event = {
                    "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
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
                producer.send(CURRENT_WATER_LEVEL_TOPIC, value=event)
                sent_events += 1

        producer.flush()
        logger.info(f"Sent {sent_events} water level events.")


if __name__ == "__main__":
    fetcher = WaterLevelFetcher(
        wakeup_topic="fetch-water-levels", group_id="water-level-fetcher"
    )
    fetcher.run()
