import os
import json
import logging
import requests

from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_TRIGGER = "fetch-public-alerts"
TOPIC_PUBLIC_ALERTS_CURRENT = "hh-public-alerts-current"

NINA_DASHBOARD_URL = (
    "https://nina.api.proxy.bund.dev/api31/dashboard/020000000000.json"
)


# -------------------------------------------------------------------
# Kafka Producer
# -------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# -------------------------------------------------------------------
# Fetch alerts from NINA
# -------------------------------------------------------------------
def fetch_nina_alerts() -> list[dict]:
    logger.info("Fetching public alerts from NINA API ...")

    resp = requests.get(NINA_DASHBOARD_URL, timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"NINA API returned HTTP {resp.status_code}")

    data = resp.json()
    logger.info(f"Fetched {len(data)} alerts.")
    return data


# -------------------------------------------------------------------
# Send alerts to Kafka
# -------------------------------------------------------------------
def send_public_alerts(alerts: list[dict]):
    sent_events = 0

    for alert in alerts:
        payload = alert.get("payload", {})
        data = payload.get("data", {})

        event = {
            "fetch_timestamp": datetime.utcnow().isoformat(),
            "source": "nina",
            "type": "public_alert",
            "alert": {
                "id": payload.get("id"),
                "hash": payload.get("hash"),
                "version": payload.get("version"),
                "provider": data.get("provider"),
                "msgType": data.get("msgType"),
                "severity": data.get("severity"),
                "urgency": data.get("urgency"),
                "headline": data.get("headline"),
                "valid": data.get("valid"),
            },
            "area": {
                "type": data.get("area", {}).get("type"),
                "codes": data.get("area", {}).get("data"),
            },
            "timing": {
                "sent": alert.get("sent"),
                "onset": alert.get("onset"),
                "effective": alert.get("effective"),
                "expires": alert.get("expires"),
            },
            "i18nTitle": alert.get("i18nTitle"),
        }

        logger.info(
            f"Sending public alert: "
            f"{data.get('headline')} "
            f"(provider={data.get('provider')}, "
            f"severity={data.get('severity')}, "
            f"msgType={data.get('msgType')})"
        )

        producer.send(TOPIC_PUBLIC_ALERTS_CURRENT, value=event)
        sent_events += 1

    producer.flush()
    logger.info(f"Sent {sent_events} public alert events.")


# -------------------------------------------------------------------
# Processing pipeline
# -------------------------------------------------------------------
def process_public_alerts():
    try:
        logger.info(f"Public alert fetch job started at {datetime.now()}")
        alerts = fetch_nina_alerts()
        send_public_alerts(alerts)
        logger.info("Public alert fetch cycle completed.")
    except Exception as e:
        logger.error(f"Error during public alert processing: {e}")


# -------------------------------------------------------------------
# Kafka trigger consumer
# -------------------------------------------------------------------
def main():
    logger.info("Public Alert Fetcher started – waiting for Kafka trigger")

    consumer = KafkaConsumer(
        TOPIC_TRIGGER,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="public-alert-fetcher",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest"
    )

    for message in consumer:
        logger.info(f"Received fetch trigger: {message.value}")
        process_public_alerts()


# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
