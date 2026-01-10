import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("KafkaTest") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

logger.info("Testing BBC News...")
df_bbc_test = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bbc-europe-news") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

count = df_bbc_test.count()
logger.info(f"BBC News total messages: {count}")

if count > 0:
    df_bbc_test.selectExpr("CAST(value AS STRING)").show(5, truncate=False)

logger.info("Testing NYT Europe...")
df_nyt_eu_test = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nyt-europe-news") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

count = df_nyt_eu_test.count()
logger.info(f"NYT Europe total messages: {count}")

logger.info("Testing NYT World...")
df_nyt_world_test = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nyt-world-news") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

count = df_nyt_world_test.count()
logger.info(f"NYT World total messages: {count}")

topics = [
    "hh-air-pollution-current",
    "hh-weather-current",
    "hh-traffic-data",
    "hh-public-alerts-current",
    "hh-street-construction",
    "hh-water-level-current"
]

for topic in topics:
    logger.info(f"Testing {topic}...")
    df_test = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    count = df_test.count()
    logger.info(f"{topic}: {count} messages")

logger.info("Test complete!")