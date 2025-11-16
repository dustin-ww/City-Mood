from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("CityMoodToPostgres") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()

kafka_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("feature_index", IntegerType(), True),
    StructField("feature", StringType(), True)
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hh-traffic-data") \
    .option("startingOffsets", "latest") \
    .load() \
    .drop("timestamp")



parsed_df = df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*") \
 .withColumnRenamed("fetch_timestamp", "event_meta_timestamp")   


scored_df = parsed_df \
    .groupBy("source") \
    .count() \
    .withColumnRenamed("count", "feature_count") \
    .withColumn("score", col("feature_count").cast(DoubleType()))


overall_score = scored_df.agg(
    avg("score").alias("avg_score")
)


# --- WRITE TO POSTGRES ---
def write_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        (
            batch_df
            .withColumn("updated_at", current_timestamp())
            .withColumn("metric_name", col("avg_score"))
            .write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/city_mood")
            .option("dbtable", "mood_aggregates")
            .option("user", "spark")
            .option("password", "spark")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        print(f"Batch {batch_id} geschrieben")


query = (
    overall_score.writeStream
    .outputMode("complete")
    .foreachBatch(write_to_postgres)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
