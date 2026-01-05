from datetime import datetime
from common.base_fetcher import BaseFetcher
from common.common_utils import logger, get_kafka_producer, get_redis_client, get_fetch_interval, get_last_timestamp, set_last_timestamp
import requests

# SERVICE SPECIFIC CONFIG
NINA_DASHBOARD_URL = "https://nina.api.proxy.bund.dev/api31/dashboard/020000000000.json"
TOPIC_PUBLIC_ALERTS_CURRENT = "hh-public-alerts-current"

# Optional Redis keys
REDIS_LAST_FETCH_KEY = "public-alert:last_fetch"


class PublicAlertFetcher(BaseFetcher):

    def process_message(self, message: dict):
        logger.info("Public alert fetch job started")
        self.process_public_alerts()
        logger.info("Public alert fetch job completed")

    def fetch_nina_alerts(self) -> list[dict]:
        logger.info("Fetching public alerts from NINA API ...")
        resp = requests.get(NINA_DASHBOARD_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"Fetched {len(data)} alerts.")
        return data

    def send_public_alerts(self, alerts: list[dict]):
        producer = get_kafka_producer()
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

    def process_public_alerts(self):
        """ 
        last_fetch = get_last_timestamp(REDIS_LAST_FETCH_KEY)
        interval = get_fetch_interval("public-alert:fetch_interval")
        now = datetime.utcnow()
        if last_fetch and (now - last_fetch).total_seconds() < interval:
            logger.info(
                f"Skipping public alert fetch. Only {(now - last_fetch).total_seconds()/3600:.2f}h since last fetch. "
                f"Required interval: {interval/3600:.2f}h"
            )
            return """

        try:
            alerts = self.fetch_nina_alerts()
            self.send_public_alerts(alerts)
            set_last_timestamp(REDIS_LAST_FETCH_KEY, now)
            logger.info("Public alert fetch cycle completed.")
        except Exception as e:
            logger.error(f"Error during public alert processing: {e}")


if __name__ == "__main__":
    fetcher = PublicAlertFetcher(wakeup_topic="fetch-public-alerts", group_id="public-alert-fetcher")
    fetcher.run()
