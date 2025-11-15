from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import time

spark = SparkSession.builder \
    .appName("CityMoodToPostgres") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
    .getOrCreate()

schema = "timestamp STRING, city STRING, mood STRING, score DOUBLE"

df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv("/opt/spark-data/input")

agg = df.groupBy("city").agg(avg("score").alias("avg_mood_score"))

def write_to_postgres(batch_df, batch_id):
    (
        batch_df
        .withColumn("updated_at", df.selectExpr("current_timestamp()").limit(1).collect()[0][0])
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

query = (
    agg.writeStream
    .outputMode("complete")
    .foreachBatch(write_to_postgres)
    .trigger(processingTime="5 seconds")
    .start()
)


query.awaitTermination()
