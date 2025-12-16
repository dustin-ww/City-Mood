# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, from_json, col, current_timestamp, to_timestamp, lit, count
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# spark = SparkSession.builder \
#     .appName("CityMoodToPostgres") \
#     .master("spark://spark-master:7077") \
#     .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
#     .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
#     .getOrCreate()

# kafka_schema = StructType([
#     StructField("fetch_timestamp", StringType(), True),
#     StructField("source", StringType(), True),
#     StructField("feature_index", IntegerType(), True),
#     StructField("feature", StringType(), True)
# ])

# logger.info("Connecting to Kafka...")

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "hh-traffic-data") \
#     .option("startingOffsets", "earliest")  \
#     .load()


# logger.info("Kafka stream created")

# parsed_df = df.select(
#     from_json(col("value").cast("string"), kafka_schema).alias("data")
# ).select("data.*") \
#  .withColumn("event_time", to_timestamp("fetch_timestamp"))

# aggregated = parsed_df \
#     .withWatermark("event_time", "5 minutes") \
#     .groupBy("source") \
#     .count() \
#     .withColumnRenamed("count", "feature_count")

# # --- WRITE TO POSTGRES ---
# def write_to_postgres(batch_df, batch_id):
#     if not batch_df.isEmpty():
#         avg_score_value = batch_df.agg(avg("feature_count")).collect()[0][0]
        
#         result_df = spark.createDataFrame(
#             [(float(avg_score_value) if avg_score_value else 0.0, )],
#             ["avg_score"]
#         )
        
#         (
#             result_df
#             .withColumn("metric_name", lit("avg_score"))
#             .withColumn("updated_at", current_timestamp())
#             .select("avg_score", "updated_at", "metric_name")
#             .write
#             .format("jdbc")
#             .option("url", "jdbc:postgresql://postgres:5432/city_mood")
#             .option("dbtable", "mood_aggregates")
#             .option("user", "spark")
#             .option("password", "spark")
#             .option("driver", "org.postgresql.Driver")
#             .mode("append")
#             .save()
#         )

# query = (
#     aggregated.writeStream
#     .outputMode("complete")
#     .foreachBatch(write_to_postgres)
#     .trigger(processingTime="5 seconds")
#     .start()
# )

# query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date, count, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -- 1. Spark Session --
spark = SparkSession.builder \
    .appName("CityMoodPipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

# -- 2. Schemas --

# Schema für Traffic (Features)
traffic_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("feature", StringType(), True) # GeoJSON String
])

# Schema für Events (Kultur & Transparenz)
event_schema = StructType([
    StructField("source", StringType(), True),
    StructField("fetch_timestamp", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category", ArrayType(StringType()), True) # Liste von Tags/Kategorien
])


# -- 3. Streams lesen --

# Stream A: Traffic
df_traffic = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hh-traffic-data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), traffic_schema).alias("data")) \
    .select("data.*") \
    .withColumn("day_date", to_date(to_timestamp(col("fetch_timestamp")))) \
    .withColumn("category", lit("traffic_feature"))

# Stream B: Events (Kultur + Transparenz)
df_events = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hh-kultur-events,hh-transparenz-events") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
    .select("data.*") \
    .withColumn("day_date", to_date(to_timestamp(col("fetch_timestamp")))) \
    .withColumn("category_str", col("category").cast("string")) # Einfachheit halber als String


# -- 4. Union aller Streams (für einfache Counts) --
# Wir normalisieren auf: day_date, source, category
common_df = df_traffic.select("day_date", "source", "category") \
    .union(df_events.select("day_date", "source", col("category_str").alias("category")))

# Aggregation: Count pro Tag, Source, Category
counts_df = common_df \
    .withWatermark("day_date", "1 day") \
    .groupBy("day_date", "source", "category") \
    .count() \
    .withColumnRenamed("count", "event_count")


# -- 5. Write to Postgres Function --
def write_counts_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        # Schreibe in daily_source_counts
        (
            batch_df
            .withColumn("updated_at", current_timestamp())
            .write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/city_mood")
            .option("dbtable", "daily_source_counts")
            .option("user", "spark")
            .option("password", "spark")
            .option("driver", "org.postgresql.Driver")
            .mode("append") # Achtung: Im echten Betrieb eher Upsert nötig, für Demo reicht Append (gibt halt Duplikate beim Restart)
            .save()
        )
        # Hier könnte man jetzt auch den Daily Mood berechnen und in daily_mood_score schreiben
        # (Logik: Traffic=Negativ, Kultur=Positiv, Transparenz=Neutral/Negativ)

# -- 6. Start Query --
query = (
    counts_df.writeStream
    .outputMode("update") # Update mode, damit wir laufende Zähler bekommen
    .foreachBatch(write_counts_to_postgres)
    .trigger(processingTime="10 seconds")
    .start()
)

logger.info("Streaming Query started...")
query.awaitTermination()