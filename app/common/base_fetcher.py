from kafka import KafkaConsumer
import json
from common.common_utils import logger, get_kafka_producer, KAFKA_BOOTSTRAP_SERVERS

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
            auto_offset_reset="latest"
        )

    def process_message(self, message: dict):
        raise NotImplementedError()

    def run(self):
        logger.info(f"{self.__class__.__name__} started – waiting for Kafka events on topic {self.wakeup_topic}")
        for msg in self.consumer:
            event = msg.value
            logger.info(f"Received fetch trigger: {event}")
            try:
                self.process_message(event)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
