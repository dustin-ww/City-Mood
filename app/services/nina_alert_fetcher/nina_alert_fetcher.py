from datetime import datetime, timezone
from common.base_fetcher import BaseFetcher
from common.common_utils import (
    logger,
    get_kafka_producer,
    get_fetch_interval,
    get_last_timestamp,
    set_last_timestamp,
    get_redis_client
)
import requests


# SERVICE SPECIFIC CONFIG
NINA_DASHBOARD_URL = "https://nina.api.proxy.bund.dev/api31/dashboard/020000000000.json"
TOPIC_PUBLIC_ALERTS_CURRENT = "hh-public-alerts-current"

REDIS_LAST_FETCH_KEY = "public-alert:last_fetch"
REDIS_PROCESSED_HASHES_KEY = "public-alert:processed_hashes"



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
        r = get_redis_client()

        sent_events = 0
        skipped_duplicates = 0

        for alert in alerts:
            payload = alert.get("payload", {})
            data = payload.get("data", {})

            alert_hash = payload.get("hash")
            if not alert_hash:
                logger.warning("Skipping alert without hash")
                continue

            # Check for duplicates
            if r.sismember(REDIS_PROCESSED_HASHES_KEY, alert_hash):
                skipped_duplicates += 1
                logger.debug(
                    f"Skipping duplicate public alert "
                    f"(hash={alert_hash}, headline={data.get('headline')})"
                )
                continue

            event = {
                "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
                "source": "nina",
                "type": "public_alert",
                "alert": {
                    "id": payload.get("id"),
                    "hash": alert_hash,
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
            }

            logger.info(
                "Sending public alert: "
                f"{data.get('headline')} "
                f"(provider={data.get('provider')}, "
                f"severity={data.get('severity')}, "
                f"msgType={data.get('msgType')})"
            )

            producer.send(TOPIC_PUBLIC_ALERTS_CURRENT, value=event)

            r.sadd(REDIS_PROCESSED_HASHES_KEY, alert_hash)
            sent_events += 1

        producer.flush()

        logger.info(
            f"Public alerts sent: {sent_events}, "
            f"duplicates skipped: {skipped_duplicates}"
        )


    def process_public_alerts(self):
        now = datetime.now(timezone.utc)
        interval = get_fetch_interval()  # Sekunden
        last_fetch = get_last_timestamp(REDIS_LAST_FETCH_KEY)

        if last_fetch:
            elapsed = (now - last_fetch).total_seconds()
            if elapsed < interval:
                logger.info(
                    f"Skipping public alert fetch. "
                    f"{elapsed/3600:.2f}h elapsed, "
                    f"required {interval/3600:.2f}h"
                )
                return

        try:
            alerts = self.fetch_nina_alerts()
            self.send_public_alerts(alerts)

            set_last_timestamp(REDIS_LAST_FETCH_KEY, now)
            logger.info("Public alert fetch cycle completed.")

        except Exception as e:
            logger.error(f"Error during public alert processing: {e}")


if __name__ == "__main__":
    fetcher = PublicAlertFetcher(
        wakeup_topic="fetch-public-alerts",
        group_id="public-alert-fetcher",
    )
    fetcher.run()
