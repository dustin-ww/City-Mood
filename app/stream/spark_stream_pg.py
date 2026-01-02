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

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import great_expectations as gx
import os
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -- 1. Spark Session --
spark = SparkSession.builder \
    .appName("CityMoodPipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

# -- 2. Schemas --
traffic_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("feature", StringType(), True)
])

event_schema = StructType([
    StructField("source", StringType(), True),
    StructField("fetch_timestamp", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category", ArrayType(StringType()), True)
])

# -- 3. Streams lesen --
df_traffic = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hh-traffic-data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), traffic_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
    .withColumn("day_date", to_date(col("event_time"))) \
    .withColumn("category", lit("traffic_feature"))

df_events = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hh-kultur-events,hh-transparenz-events") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
    .withColumn("day_date", to_date(col("event_time"))) \
    .withColumn("category_str", col("category").cast("string"))

# -- 4. Union & Aggregation --
common_df = df_traffic.select("event_time", "day_date", "source", "category") \
    .union(df_events.select("event_time", "day_date", "source", col("category_str").alias("category")))

counts_df = common_df \
    .withWatermark("event_time", "1 day") \
    .groupBy("day_date", "source", "category") \
    .count() \
    .withColumnRenamed("count", "event_count")

# -- 5. Write to Postgres MIT REPORTING --
def write_counts_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    logger.info(f"Processing Batch {batch_id}...")

    try:
        pdf = batch_df.toPandas()
        
        # Regel 1: Source darf nicht null sein
        null_sources = pdf["source"].isnull().sum()
        # Regel 2: Event Count muss positiv sein
        negative_counts = (pdf["event_count"] < 0).sum()
        
        if null_sources > 0 or negative_counts > 0:
            logger.warning(f"Data Quality Issues: {null_sources} null sources, {negative_counts} negative counts")
        else:
            logger.info(f"Data Quality Check PASSED for Batch {batch_id}")
            
    except Exception as e:
        logger.error(f"Quality Check Error: {e}")

    # --- REPORT FILE in gx-reports (damit du "wirklich was siehst") ---
    os.makedirs("/app/gx-reports", exist_ok=True)
    report_path = f"/app/gx-reports/report_batch_{batch_id}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "batch_id": batch_id,
                "generated_at_utc": datetime.utcnow().isoformat(),
                "rows_in_batch": int(pdf.shape[0]),
                "null_sources": int(null_sources),
                "negative_counts": int(negative_counts),
            },
            f,
            indent=2,
            default=str,
        )
    logger.info(f"Wrote report: {report_path}")
    # ... nach logger.info(f"Wrote report: {report_path}") ...

    # --- HTML REPORT in gx-reports ---
    status = "PASSED" if (null_sources == 0 and negative_counts == 0) else "FAILED"
    html_path = f"/app/gx-reports/report_batch_{batch_id}.html"

    sample_table = pdf.head(50).to_html(index=False)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Data Quality Report – Batch {batch_id}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .ok {{ color: #0a7; font-weight: 700; }}
    .bad {{ color: #c00; font-weight: 700; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
    th {{ background: #f6f6f6; }}
    .meta {{ margin: 12px 0; }}
    code {{ background: #f2f2f2; padding: 2px 6px; border-radius: 4px; }}
  </style>
</head>
<body>
  <h2>Data Quality Report</h2>
  <div class="meta">
    <div><b>Batch:</b> {batch_id}</div>
    <div><b>Generated (UTC):</b> {datetime.utcnow().isoformat()}</div>
    <div><b>Status:</b> <span class="{('ok' if status=='PASSED' else 'bad')}">{status}</span></div>
    <div><b>Rows in batch:</b> {int(pdf.shape[0])}</div>
    <div><b>null_sources:</b> {int(null_sources)}</div>
    <div><b>negative_counts:</b> {int(negative_counts)}</div>
    <div><b>JSON:</b> <code>report_batch_{batch_id}.json</code></div>
  </div>

  <h3>Sample (first 50 rows)</h3>
  {sample_table}
</body>
</html>""")

    logger.info(f"Wrote HTML report: {html_path}")

    # --- UPSERT to Postgres (kein Crash bei Duplicate Key) ---
    rows = [
        (r["day_date"], r["source"], r["category"], int(r["event_count"]), datetime.utcnow())
        for r in batch_df.select("day_date", "source", "category", "event_count").collect()
    ]

    if rows:
        conn = psycopg2.connect(host="postgres", dbname="city_mood", user="spark", password="spark")
        conn.autocommit = True
        with conn.cursor() as cur:
            sql = """
            INSERT INTO daily_source_counts (day_date, source, category, event_count, updated_at)
            VALUES %s
            ON CONFLICT (day_date, source, category)
            DO UPDATE SET event_count = EXCLUDED.event_count,
                          updated_at  = EXCLUDED.updated_at;
            """
            execute_values(cur, sql, rows)
        conn.close()

# -- 6. Start Query --
query = (
    counts_df.writeStream
    .outputMode("update") 
    .foreachBatch(write_counts_to_postgres)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()