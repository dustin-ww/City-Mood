from datetime import datetime
from common.base_fetcher import BaseFetcher
from common.common_utils import logger, get_kafka_producer, get_redis_client
import requests

# SERVICE SPECIFIC CONFIG
OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=53.5507&longitude=9.993"
    "&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
    "precipitation,rain,showers,snowfall,weather_code,surface_pressure,"
    "pressure_msl,cloud_cover,wind_speed_10m,wind_direction_10m,wind_gusts_10m"
    "&hourly=temperature_2m,rain,snowfall,visibility,relative_humidity_2m,"
    "precipitation,cloud_cover,uv_index,sunshine_duration,wind_speed_10m"
    "&daily=sunrise,sunset,uv_index_max,daylight_duration,sunshine_duration,"
    "rain_sum,snowfall_sum,uv_index_clear_sky_max,temperature_2m_max,"
    "temperature_2m_min,apparent_temperature_max,apparent_temperature_min"
)
CURRENT_WEATHER_TOPIC = "hh-weather-current"
DAILY_WEATHER_TOPIC = "hh-weather-daily"


class WeatherFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("Weather fetch job started")
        try:
            data = self.fetch_weather_data()
            self.send_current_weather(data)
            self.send_daily_weather(data)
            logger.info("Weather fetch job completed")
        except Exception as e:
            logger.error(f"Error during weather fetch: {e}")

    def fetch_weather_data(self):
        logger.info("Fetching weather data from Open-Meteo ...")
        resp = requests.get(OPEN_METEO_URL, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def send_current_weather(self, data: dict):
        producer = get_kafka_producer()
        event = {
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "source": "open-meteo",
            "type": "current_weather",
            "timezone": data.get("timezone"),
            "location": {
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "elevation": data.get("elevation")
            },
            "current": data.get("current")
        }
        producer.send(CURRENT_WEATHER_TOPIC, value=event)
        producer.flush()
        logger.info("Current weather sent.")

    def send_daily_weather(self, data: dict):
        producer = get_kafka_producer()
        event = {
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "source": "open-meteo",
            "type": "daily_weather",
            "timezone": data.get("timezone"),
            "location": {
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "elevation": data.get("elevation")
            },
            "daily": data.get("daily")
        }
        producer.send(DAILY_WEATHER_TOPIC, value=event)
        producer.flush()
        logger.info("Daily weather sent.")


if __name__ == "__main__":
    fetcher = WeatherFetcher(wakeup_topic="fetch-weather", group_id="weather-fetcher")
    fetcher.run()
