import os
import json
import logging
import redis
from kafka import KafkaProducer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetcher")

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def get_kafka_producer():
    """Singleton Kafka Producer"""
    logger.info("Initializing Kafka Producer")
    global _producer
    try:
        return _producer
    except NameError:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        return _producer

# Redis Config
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

def get_redis_client():
    """Singleton Redis Client"""
    global _redis
    try:
        return _redis
    except NameError:
        _redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        return _redis

# Fetch Interval
FETCH_INTERVAL_HOURS = float(os.getenv("API_REFRESH_INTERVAL_HOURS", 3))
FETCH_INTERVAL = int(FETCH_INTERVAL_HOURS * 3600)

def get_fetch_interval(redis_key: str = "rss:fetch_interval") -> int:
    # r = get_redis_client()
    # val = r.get(redis_key)
    # if val:
    #     try:
    #         return int(val)
    #     except ValueError:
    #         logger.warning(f"Invalid fetch_interval in Redis: {val}, using ENV default")
    return FETCH_INTERVAL

# Redis Helpers
def is_duplicate(key_set: str, entry_id: str) -> bool:
    r = get_redis_client()
    return r.sismember(key_set, entry_id)

def mark_processed(key_set: str, entry_id: str, ttl: int):
    r = get_redis_client()
    r.sadd(key_set, entry_id)
    r.expire(key_set, ttl)

def get_last_timestamp(key: str) -> datetime | None:
    r = get_redis_client()
    ts = r.get(key)
    if ts:
        return datetime.fromisoformat(ts)
    return None

def set_last_timestamp(key: str, ts: datetime):
    r = get_redis_client()
    r.set(key, ts.isoformat())
