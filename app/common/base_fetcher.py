from kafka import KafkaConsumer
import json
from common.common_utils import logger, get_redis_client, get_fetch_interval, KAFKA_BOOTSTRAP_SERVERS

class BaseFetcher:

    wakeup_topic: str
    group_id: str

    def __init__(self, wakeup_topic: str, group_id: str):
        self.wakeup_topic = wakeup_topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            self.wakeup_topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),   
            auto_offset_reset="latest",
            enable_auto_commit=False, # do not auto-commit offsets (to prohibit execution of missed events on restart)
        )

        self.consumer.poll(timeout_ms=1000)  # Hol die Partitionen
        for tp in self.consumer.assignment(): # Für jede Partition
            self.consumer.seek_to_end(tp)     # springe ans Ende, alte Nachrichten ignorieren


    def process_message(self, message: dict):
        raise NotImplementedError()

    def run(self):
        logger.info(f"{self.__class__.__name__} started – waiting for Kafka events on topic {self.wakeup_topic}")
        redis = get_redis_client()
        interval = max(1, int(get_fetch_interval()))
        lock_key = f"fetcher:{self.wakeup_topic}:lock"

        for msg in self.consumer:
            event = msg.value
            logger.info(f"Received fetch trigger: {event}")

            if not redis.set(lock_key, "1", nx=True, ex=interval):
                logger.info("Fetcher locked – skipping trigger")
                continue

            try:
                self.process_message(event)
                #self.consumer.commit()
            except Exception as e:
                logger.error(f"Error processing message: {e}")

