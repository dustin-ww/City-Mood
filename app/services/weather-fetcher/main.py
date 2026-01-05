import os
import json
import time
import schedule
import requests
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime, date
import redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment / Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_CURRENT = "hh-weather-current"
TOPIC_DAILY = "hh-weather-daily"

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_KEY = "weather:daily:last_sent"

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

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Redis Client
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def get_last_daily_sent_date() -> date | None:
    val = r.get(REDIS_KEY)
    if val:
        return date.fromisoformat(val)
    return None

def set_last_daily_sent_date(d: date):
    r.set(REDIS_KEY, d.isoformat())

# Weather Functions
def fetch_weather_data() -> dict:
    logger.info("Fetching weather data from Open-Meteo ...")
    resp = requests.get(OPEN_METEO_URL, timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"Open-Meteo returned HTTP {resp.status_code}")
    data = resp.json()
    logger.info("Weather data fetched successfully.")
    return data

def send_current_weather(data: dict):
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
    logger.info("Sending current weather event to Kafka ...")
    producer.send(TOPIC_CURRENT, value=event)
    producer.flush()
    logger.info("Current weather sent.")

def send_daily_weather(data: dict):
    today = date.today()
    last_sent = get_last_daily_sent_date()

    if last_sent == today:
        logger.info("Daily weather already sent today. Skipping.")
        return

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

    logger.info("Sending daily weather event to Kafka ...")
    producer.send(TOPIC_DAILY, value=event)
    producer.flush()
    logger.info("Daily weather sent.")

    set_last_daily_sent_date(today)

def process_weather():
    try:
        logger.info(f"Weather fetch job started at {datetime.now()}")
        data = fetch_weather_data()
        send_current_weather(data)
        send_daily_weather(data)
        logger.info("Weather fetch cycle completed.")
    except Exception as e:
        logger.error(f"Error during weather processing: {e}")


def main():
    logger.info("Weather Fetcher started – waiting for Kafka events")
    consumer = KafkaConsumer(
        "fetch-weather",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="weather-fetcher",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest"
    )

    for message in consumer:
        event = message.value
        logger.info(f"Received fetch trigger: {event}")
        process_weather()

if __name__ == "__main__":
    main()
