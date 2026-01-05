from datetime import datetime, date
from common.base_fetcher import BaseFetcher
from common.common_utils import logger, get_kafka_producer, get_redis_client, get_fetch_interval, get_last_timestamp, set_last_timestamp
import requests

# SERVICE SPECIFIC CONFIG
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

TOPIC_CURRENT = "hh-air-pollution-current"
TOPIC_DAILY = "hh-air-pollution-daily"

REDIS_DAILY_KEY = "air-pollution:daily:last_sent"


class AirPollutionFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("Air pollution fetch job started")
        self.process_air_pollution()
        logger.info("Air pollution fetch job completed")

    def fetch_air_pollution_data(self) -> dict:
        logger.info("Fetching air pollution data from Open-Meteo ...")
        resp = requests.get(OPEN_METEO_URL, timeout=15)
        resp.raise_for_status()
        logger.info("Air pollution data fetched successfully.")
        return resp.json()

    def send_current_air_pollution(self, data: dict):
        producer = get_kafka_producer()
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
        producer.send(TOPIC_CURRENT, value=event)
        producer.flush()
        logger.info("Current air pollution sent.")

    def send_daily_air_pollution(self, data: dict):
        r = get_redis_client()
        today = date.today()
        last_sent_str = r.get(REDIS_DAILY_KEY)
        last_sent = date.fromisoformat(last_sent_str) if last_sent_str else None

        if last_sent == today:
            logger.info("Daily air pollution already sent today. Skipping...")
            return

        producer = get_kafka_producer()
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
        producer.send(TOPIC_DAILY, value=event)
        producer.flush()
        r.set(REDIS_DAILY_KEY, today.isoformat())
        logger.info("Daily air pollution sent.")

    def process_air_pollution(self):
        # # Optional: Fetch interval check via Redis
        # last_fetch = get_last_timestamp("air-pollution:last_fetch")
        # interval = get_fetch_interval("air-pollution:fetch_interval")
        # now = datetime.utcnow()
        # if last_fetch and (now - last_fetch).total_seconds() < interval:
        #     logger.info(
        #         f"Skipping air pollution fetch. Only {(now - last_fetch).total_seconds()/3600:.2f}h since last fetch. "
        #         f"Required interval: {interval/3600:.2f}h"
        #     )
        #     return

        try:
            data = self.fetch_air_pollution_data()
            self.send_current_air_pollution(data)
            self.send_daily_air_pollution(data)
            set_last_timestamp("air-pollution:last_fetch", now)
            logger.info("Air pollution fetch cycle completed.")
        except Exception as e:
            logger.error(f"Error during air pollution processing: {e}")


if __name__ == "__main__":
    fetcher = AirPollutionFetcher(wakeup_topic="fetch-air-pollution", group_id="air-pollution-fetcher")
    fetcher.run()
