import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, lit, avg, count, sum as spark_sum,
    when, coalesce, window, current_timestamp, expr, least, greatest,
    unix_timestamp, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    BooleanType, ArrayType, LongType
)
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark Session Init
spark = SparkSession.builder \
    .appName("CityMoodPipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", 
            "org.postgresql:postgresql:42.7.8,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .config("spark.executor.memory", "10g") \       
    .config("spark.executor.cores", "6") \   
    .getOrCreate()

# Schemes for all Kafka Topcis 

# News Scheme (BBC, NYT)
news_schema = StructType([
    StructField("source", StringType(), True),
    StructField("section", StringType(), True),
    StructField("fetch_timestamp", StringType(), True),
    StructField("title", StringType(), True),
    StructField("headline_sentiment", StructType([
        StructField("label", StringType(), True),
        StructField("score", DoubleType(), True)
    ]), True)
])

# Air Quality Scheme
air_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("current", StructType([
        StructField("european_aqi", IntegerType(), True),
        StructField("pm2_5", DoubleType(), True),
        StructField("pm10", DoubleType(), True)
    ]), True)
])

# Weather Scheme
weather_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("current", StructType([
        StructField("temperature_2m", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("wind_speed_10m", DoubleType(), True),
        StructField("relative_humidity_2m", IntegerType(), True)
    ]), True)
])

# Traffic Scheme
traffic_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("properties", StructType([
        StructField("zustandsklasse", StringType(), True),
        StructField("strassenklasse", StringType(), True)
    ]), True)
])

# Public Alerts Scheme
alerts_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("alert", StructType([
        StructField("severity", StringType(), True),
        StructField("valid", BooleanType(), True),
        StructField("headline", StringType(), True)
    ]), True)
])

# Construction Scheme
construction_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("properties", StructType([
        StructField("iststoerung", BooleanType(), True),
        StructField("titel", StringType(), True)
    ]), True)
])

# Water Level Scheme
water_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("measurement", StructType([
        StructField("value_cm", DoubleType(), True)
    ]), True),
    StructField("station", StructType([
        StructField("water", StringType(), True)
    ]), True)
])

# Events Scheme
events_schema = StructType([
    StructField("source", StringType(), True),
    StructField("fetch_timestamp", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category", ArrayType(StringType()), True)
])

# Read kafka streams and parse

def read_kafka_stream(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("fetch_timestamp")))

# News Streams
logger.info("Setting up NEWS streams...")
df_bbc = read_kafka_stream("bbc-europe-news", news_schema)
df_nyt_europe = read_kafka_stream("nyt-europe-news", news_schema)
df_nyt_world = read_kafka_stream("nyt-world-news", news_schema)

# Combine all news streams
df_news_all = df_bbc.union(df_nyt_europe).union(df_nyt_world)

# Calculate news score 
df_news_scored = df_news_all.withColumn(
    "news_sentiment_score",
    when(col("headline_sentiment.label") == "POSITIVE", col("headline_sentiment.score"))
    .when(col("headline_sentiment.label") == "NEUTRAL", lit(0.5))
    .otherwise(lit(1.0) - col("headline_sentiment.score"))  # NEGATIVE invertieren
)

# Air Quality Stream
logger.info("Setting up AIR QUALITY stream...")
df_air = read_kafka_stream("hh-air-pollution-current", air_schema)

df_air_scored = df_air.withColumn(
    "air_quality_score",
    when(col("current.european_aqi") <= 50, lit(1.0))
    .when(col("current.european_aqi") <= 100, 
          lit(0.7) - ((col("current.european_aqi") - 50) * 0.006))
    .when(col("current.european_aqi") <= 150, 
          lit(0.4) - ((col("current.european_aqi") - 100) * 0.006))
    .otherwise(lit(0.1))
)

# Weather Stream
logger.info("Setting up WEATHER stream...")
df_weather = read_kafka_stream("hh-weather-current", weather_schema)

df_weather_scored = df_weather.withColumn(
    "temp_score",
    when((col("current.temperature_2m") >= 15) & (col("current.temperature_2m") <= 25), lit(1.0))
    .when((col("current.temperature_2m") >= 10) & (col("current.temperature_2m") < 15), lit(0.8))
    .when((col("current.temperature_2m") > 25) & (col("current.temperature_2m") <= 30), lit(0.8))
    .when((col("current.temperature_2m") >= 5) & (col("current.temperature_2m") < 10), lit(0.6))
    .when((col("current.temperature_2m") >= -5) & (col("current.temperature_2m") < 5), lit(0.4))
    .otherwise(lit(0.2))
).withColumn(
    "weather_code_score",
    when(col("current.weather_code") <= 1, lit(1.0))
    .when(col("current.weather_code") <= 3, lit(0.8))
    .when(col("current.weather_code") <= 48, lit(0.6))
    .when(col("current.weather_code") <= 67, lit(0.5))
    .when(col("current.weather_code") <= 77, lit(0.4))
    .otherwise(lit(0.2))
).withColumn(
    "precip_score",
    when(col("current.precipitation") == 0, lit(1.0))
    .when(col("current.precipitation") < 2, lit(0.8))
    .when(col("current.precipitation") < 5, lit(0.6))
    .otherwise(lit(0.4))
).withColumn(
    "wind_score",
    when(col("current.wind_speed_10m") < 15, lit(1.0))
    .when(col("current.wind_speed_10m") < 25, lit(0.7))
    .otherwise(lit(0.4))
).withColumn(
    "weather_score",
    (col("temp_score") * 0.4 + 
     col("weather_code_score") * 0.3 + 
     col("precip_score") * 0.2 + 
     col("wind_score") * 0.1)
)

# Traffic Stream
logger.info("Setting up TRAFFIC stream...")
df_traffic = read_kafka_stream("hh-traffic-data", traffic_schema)

df_traffic_scored = df_traffic.withColumn(
    "traffic_score",
    when(col("properties.zustandsklasse") == "fliessend", lit(1.0))
    .when(col("properties.zustandsklasse") == "zaehfliessend", lit(0.6))
    .when(col("properties.zustandsklasse") == "stockend", lit(0.3))
    .otherwise(lit(0.1))  # Stau
)

# Public Alerts Stream
logger.info("Setting up ALERTS stream...")
df_alerts = read_kafka_stream("hh-public-alerts-current", alerts_schema)

df_alerts_scored = df_alerts \
    .filter(col("alert.valid") == True) \
    .withColumn(
        "alert_impact",
        when(col("alert.severity") == "Minor", lit(0.9))
        .when(col("alert.severity") == "Moderate", lit(0.7))
        .when(col("alert.severity") == "Severe", lit(0.4))
        .otherwise(lit(0.1))  # Extreme
    )

# Construction Stream
logger.info("Setting up CONSTRUCTION stream...")
df_construction = read_kafka_stream("hh-street-construction", construction_schema)

df_construction_filtered = df_construction \
    .filter(col("properties.iststoerung") == True)

# Water Level Stream
logger.info("Setting up WATER LEVEL stream...")
df_water = read_kafka_stream("hh-water-level-current", water_schema)

df_water_scored = df_water \
    .filter(col("station.water") == "ELBE") \
    .withColumn(
        "water_score",
        when((col("measurement.value_cm") >= 400) & (col("measurement.value_cm") <= 700), lit(1.0))
        .when((col("measurement.value_cm") > 700) & (col("measurement.value_cm") <= 800), lit(0.7))
        .when(col("measurement.value_cm") > 800, lit(0.3))
        .when((col("measurement.value_cm") >= 300) & (col("measurement.value_cm") < 400), lit(0.8))
        .otherwise(lit(0.5))
    )

# Events Stream (for Event counts)
logger.info("Setting up EVENTS stream...")
df_events = read_kafka_stream("hh-kultur-events,hh-transparenz-events", events_schema)

# Aggregation of streams with time windows

# News: 30-Minuten Fenster, 5-Minuten Slide
news_agg = df_news_scored \
    .withWatermark("event_time", "12 hour") \
    .groupBy(window("event_time", "30 minutes", "5 minutes")) \
    .agg(
        avg("news_sentiment_score").alias("news_score"),
        count("*").alias("news_count")
    )

# Air Quality: Letzter Wert pro 10 Minuten
air_agg = df_air_scored \
    .withWatermark("event_time", "1 hour") \
    .groupBy(window("event_time", "10 minutes", "5 minutes")) \
    .agg(
        avg("air_quality_score").alias("air_score"),
        avg("current.european_aqi").alias("avg_aqi")
    )

# Weather: Letzter Wert pro 15 Minuten
weather_agg = df_weather_scored \
    .withWatermark("event_time", "1 hour") \
    .groupBy(window("event_time", "15 minutes", "5 minutes")) \
    .agg(
        avg("weather_score").alias("weather_score"),
        avg("current.temperature_2m").alias("avg_temp")
    )

# Traffic: 15-Minuten Durchschnitt
traffic_agg = df_traffic_scored \
    .withWatermark("event_time", "30 minutes") \
    .groupBy(window("event_time", "15 minutes", "5 minutes")) \
    .agg(
        avg("traffic_score").alias("traffic_score"),
        count("*").alias("traffic_measurements")
    )

# Alerts: 30-Minuten Fenster - Count und Durchschnitt
alerts_agg = df_alerts_scored \
    .withWatermark("event_time", "1 hour") \
    .groupBy(window("event_time", "30 minutes", "5 minutes")) \
    .agg(
        count("*").alias("alert_count"),
        avg("alert_impact").alias("avg_alert_impact")
    ).withColumn(
        "alerts_score",
        when(col("alert_count") == 0, lit(1.0))
        .otherwise(
            col("avg_alert_impact") * (lit(1.0) - least(col("alert_count") * 0.1, lit(0.3)))
        )
    )

# Construction: 1-Stunden Count
construction_agg = df_construction_filtered \
    .withWatermark("event_time", "2 hours") \
    .groupBy(window("event_time", "1 hour", "15 minutes")) \
    .agg(
        count("*").alias("construction_count")
    ).withColumn(
        "construction_score",
        when(col("construction_count") == 0, lit(1.0))
        .when(col("construction_count") <= 5, lit(0.9))
        .when(col("construction_count") <= 15, lit(0.7))
        .otherwise(lit(0.5))
    )

# Water: Durchschnitt pro 30 Minuten
water_agg = df_water_scored \
    .withWatermark("event_time", "1 hour") \
    .groupBy(window("event_time", "30 minutes", "5 minutes")) \
    .agg(
        avg("water_score").alias("water_score"),
        avg("measurement.value_cm").alias("avg_water_level")
    )

# Combine all scores into CITY MOOD SCORE

logger.info("Combining all scores into CITY MOOD SCORE...")

# Basis: News Aggregation Window
city_mood_base = news_agg.select(
    col("window"),
    col("news_score"),
    col("news_count")
)

# Joins mit allen anderen Komponenten
city_mood = city_mood_base \
    .join(air_agg, "window", "left_outer") \
    .join(weather_agg, "window", "left_outer") \
    .join(traffic_agg, "window", "left_outer") \
    .join(alerts_agg, "window", "left_outer") \
    .join(construction_agg, "window", "left_outer") \
    .join(water_agg, "window", "left_outer")

# Finale City Mood Score Berechnung mit Defaults
city_mood_final = city_mood.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    coalesce(col("news_score"), lit(0.5)).alias("news_score"),
    coalesce(col("air_score"), lit(0.7)).alias("air_score"),
    coalesce(col("weather_score"), lit(0.6)).alias("weather_score"),
    coalesce(col("traffic_score"), lit(0.8)).alias("traffic_score"),
    coalesce(col("alerts_score"), lit(1.0)).alias("alerts_score"),
    coalesce(col("construction_score"), lit(0.8)).alias("construction_score"),
    coalesce(col("water_score"), lit(1.0)).alias("water_score"),
    col("news_count"),
    col("avg_aqi"),
    col("avg_temp"),
    col("traffic_measurements"),
    col("alert_count"),
    col("construction_count"),
    col("avg_water_level")
).withColumn(
    "city_mood_score",
    # Gewichtete Summe aller Komponenten
    coalesce(col("news_score"), lit(0.5)) * 0.25 +
    coalesce(col("air_score"), lit(0.7)) * 0.15 +
    coalesce(col("weather_score"), lit(0.6)) * 0.20 +
    coalesce(col("traffic_score"), lit(0.8)) * 0.15 +
    coalesce(col("alerts_score"), lit(1.0)) * 0.15 +
    coalesce(col("construction_score"), lit(0.8)) * 0.05 +
    coalesce(col("water_score"), lit(1.0)) * 0.05
).withColumn(
    "computed_at", current_timestamp()
)

#  Data quality validation with great expectations
def validate_city_mood_quality(pdf, batch_id):
    """
    Data Quality Checks für City Mood Score
    Returns: dict mit Validierungsergebnissen
    """
    issues = []
    warnings = []
    
    # CHECK 1: City Mood Score zwischen 0 und 1
    invalid_scores = ((pdf["city_mood_score"] < 0) | (pdf["city_mood_score"] > 1)).sum()
    if invalid_scores > 0:
        issues.append(f"❌ {invalid_scores} scores außerhalb [0,1]")
    
    # CHECK 2: Keine NULL Werte in Hauptscore
    null_scores = pdf["city_mood_score"].isnull().sum()
    if null_scores > 0:
        issues.append(f"❌ {null_scores} NULL city_mood_scores")
    
    # CHECK 3: Alle Komponenten-Scores zwischen 0 und 1
    for col in ["news_score", "air_score", "weather_score", "traffic_score", 
                "alerts_score", "construction_score", "water_score"]:
        invalid = ((pdf[col] < 0) | (pdf[col] > 1)).sum()
        if invalid > 0:
            issues.append(f"❌ {col}: {invalid} ungültige Werte")
    
    # CHECK 4: Mindestanzahl Datenpunkte für validen Score
    if pdf["news_count"].mean() < 1:
        warnings.append(f"⚠️ Zu wenige News-Datenpunkte (avg: {pdf['news_count'].mean():.1f})")
    
    if pdf["traffic_measurements"].mean() < 5:
        warnings.append(f"⚠️ Wenige Traffic-Messungen (avg: {pdf['traffic_measurements'].mean():.1f})")
    
    # CHECK 5: Plausibilitätschecks
    if not pdf["avg_temp"].isnull().all():
        temp_outliers = ((pdf["avg_temp"] < -30) | (pdf["avg_temp"] > 50)).sum()
        if temp_outliers > 0:
            warnings.append(f"⚠️ {temp_outliers} unplausible Temperaturen")
    
    if not pdf["avg_aqi"].isnull().all():
        aqi_outliers = ((pdf["avg_aqi"] < 0) | (pdf["avg_aqi"] > 500)).sum()
        if aqi_outliers > 0:
            warnings.append(f"⚠️ {aqi_outliers} unplausible AQI-Werte")
    
    # CHECK 6: Zeitstempel-Validierung
    if pdf["window_start"].isnull().any():
        issues.append("❌ NULL Zeitstempel gefunden")
    
    # CHECK 7: Duplikate
    duplicates = pdf.duplicated(subset=["window_start"]).sum()
    if duplicates > 0:
        warnings.append(f"⚠️ {duplicates} duplizierte Zeitfenster")
    
    # Gesamtstatus
    status = "PASSED" if len(issues) == 0 else "FAILED"
    
    return {
        "status": status,
        "issues": issues,
        "warnings": warnings,
        "metrics": {
            "total_rows": int(pdf.shape[0]),
            "invalid_scores": int(invalid_scores),
            "null_scores": int(null_scores),
            "avg_city_mood": float(pdf["city_mood_score"].mean()) if not pdf["city_mood_score"].isnull().all() else 0.0,
            "min_city_mood": float(pdf["city_mood_score"].min()) if not pdf["city_mood_score"].isnull().all() else 0.0,
            "max_city_mood": float(pdf["city_mood_score"].max()) if not pdf["city_mood_score"].isnull().all() else 0.0,
            "avg_news_count": float(pdf["news_count"].mean()) if "news_count" in pdf.columns else 0.0,
            "avg_traffic_measurements": float(pdf["traffic_measurements"].mean()) if "traffic_measurements" in pdf.columns else 0.0
        }
    }

# Write to postgres with data quality checks and reports
def write_city_mood_to_postgres(batch_df, batch_id):
    """
    Schreibt City Mood Score in Postgres und erstellt Quality Reports
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id} ist leer - überspringe")
        return
    
    logger.info(f"⚙️ Processing Batch {batch_id}...")
    
    try:
        pdf = batch_df.toPandas()
        
        # DATA QUALITY VALIDATION
        validation_result = validate_city_mood_quality(pdf, batch_id)
        
        # Log Results
        logger.info(f"Batch {batch_id} Status: {validation_result['status']}")
        
        if validation_result['issues']:
            for issue in validation_result['issues']:
                logger.error(issue)
        
        if validation_result['warnings']:
            for warning in validation_result['warnings']:
                logger.warning(warning)
        
        logger.info(f"Avg City Mood Score: {validation_result['metrics']['avg_city_mood']:.3f}")
        
        # JSON REPORT
        os.makedirs("/app/gx-reports", exist_ok=True)
        report_path = f"/app/gx-reports/city_mood_batch_{batch_id}.json"
        
        report_data = {
            "batch_id": batch_id,
            "generated_at_utc": datetime.utcnow().isoformat(),
            "validation": validation_result,
            "sample_data": pdf.head(10).to_dict(orient="records")
        }
        
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, default=str)
        
        logger.info(f"JSON Report: {report_path}")
        
        # HTML REPORT
        html_path = f"/app/gx-reports/city_mood_batch_{batch_id}.html"
        
        status_class = "ok" if validation_result["status"] == "PASSED" else "bad"
        issues_html = "<br>".join(validation_result["issues"]) if validation_result["issues"] else "Keine"
        warnings_html = "<br>".join(validation_result["warnings"]) if validation_result["warnings"] else "Keine"
        
        # Komponenten-Statistiken
        component_stats = ""
        for col in ["news_score", "air_score", "weather_score", "traffic_score", 
                    "alerts_score", "construction_score", "water_score"]:
            if col in pdf.columns:
                component_stats += f"""
                <tr>
                    <td><b>{col}</b></td>
                    <td>{pdf[col].mean():.3f}</td>
                    <td>{pdf[col].min():.3f}</td>
                    <td>{pdf[col].max():.3f}</td>
                </tr>
                """
        
        sample_table = pdf[[
            "window_start", "city_mood_score", "news_score", "air_score", 
            "weather_score", "traffic_score", "alerts_score"
        ]].head(20).to_html(index=False, float_format=lambda x: f"{x:.3f}")
        
        html_content = f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>City Mood Quality Report – Batch {batch_id}</title>
  <style>
    body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 24px; background: #f5f5f5; }}
    .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 32px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
    h2 {{ color: #333; border-bottom: 3px solid #0066cc; padding-bottom: 12px; }}
    h3 {{ color: #555; margin-top: 32px; }}
    .ok {{ color: #0a7; font-weight: 700; }}
    .bad {{ color: #c00; font-weight: 700; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 16px; }}
    th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
    th {{ background: #f0f0f0; font-weight: 600; }}
    .meta {{ background: #f9f9f9; padding: 16px; border-radius: 4px; margin: 16px 0; }}
    .meta div {{ margin: 8px 0; }}
    .score-box {{ display: inline-block; padding: 12px 24px; background: #e3f2fd; border-radius: 8px; font-size: 24px; font-weight: bold; color: #0066cc; margin: 16px 0; }}
    .issue-box {{ background: #ffebee; border-left: 4px solid #c00; padding: 12px; margin: 8px 0; }}
    .warning-box {{ background: #fff3e0; border-left: 4px solid #ff9800; padding: 12px; margin: 8px 0; }}
    code {{ background: #f2f2f2; padding: 2px 6px; border-radius: 4px; font-family: 'Courier New', monospace; }}
  </style>
</head>
<body>
<div class="container">
  <h2>🏙️ City Mood Quality Report</h2>
  
  <div class="meta">
    <div><b>Batch ID:</b> {batch_id}</div>
    <div><b>Generiert (UTC):</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}</div>
    <div><b>Status:</b> <span class="{status_class}">{validation_result['status']}</span></div>
    <div><b>Datensätze:</b> {validation_result['metrics']['total_rows']}</div>
  </div>
  
  <div class="score-box">
    Ø City Mood Score: {validation_result['metrics']['avg_city_mood']:.3f}
  </div>
  
  <h3>📈 Score-Statistiken</h3>
  <table>
    <tr><th>Metrik</th><th>Durchschnitt</th><th>Minimum</th><th>Maximum</th></tr>
    <tr>
      <td><b>City Mood Score</b></td>
      <td>{validation_result['metrics']['avg_city_mood']:.3f}</td>
      <td>{validation_result['metrics']['min_city_mood']:.3f}</td>
      <td>{validation_result['metrics']['max_city_mood']:.3f}</td>
    </tr>
    {component_stats}
  </table>
  
  <h3>⚠️ Validierungsergebnisse</h3>
  
  <div class="issue-box">
    <b>Issues:</b><br>{issues_html}
  </div>
  
  <div class="warning-box">
    <b>Warnings:</b><br>{warnings_html}
  </div>
  
  <h3>📋 Datenqualitäts-Metriken</h3>
  <table>
    <tr><th>Metrik</th><th>Wert</th></tr>
    <tr><td>Ungültige Scores (außerhalb [0,1])</td><td>{validation_result['metrics']['invalid_scores']}</td></tr>
    <tr><td>NULL Scores</td><td>{validation_result['metrics']['null_scores']}</td></tr>
    <tr><td>Ø Anzahl News</td><td>{validation_result['metrics']['avg_news_count']:.1f}</td></tr>
    <tr><td>Ø Traffic-Messungen</td><td>{validation_result['metrics']['avg_traffic_measurements']:.1f}</td></tr>
  </table>
  
  <h3>📊 Sample Data (erste 20 Zeilen)</h3>
  {sample_table}
  
  <div style="margin-top: 32px; color: #666; font-size: 12px;">
    <p>JSON Report: <code>city_mood_batch_{batch_id}.json</code></p>
  </div>
</div>
</body>
</html>"""
        
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        logger.info(f"HTML Report: {html_path}")
        
        # Write to Postgres
        if validation_result["status"] == "PASSED" or len(validation_result["issues"]) == 0:
            rows = []
            for _, row in pdf.iterrows():
                rows.append((
                    row["window_start"],
                    row["window_end"],
                    float(row["city_mood_score"]),
                    float(row["news_score"]) if not pd.isna(row["news_score"]) else None,
                    float(row["air_score"]) if not pd.isna(row["air_score"]) else None,
                    float(row["weather_score"]) if not pd.isna(row["weather_score"]) else None,
                    float(row["traffic_score"]) if not pd.isna(row["traffic_score"]) else None,
                    float(row["alerts_score"]) if not pd.isna(row["alerts_score"]) else None,
                    float(row["construction_score"]) if not pd.isna(row["construction_score"]) else None,
                    float(row["water_score"]) if not pd.isna(row["water_score"]) else None,
                    int(row["news_count"]) if not pd.isna(row["news_count"]) else 0,
                    datetime.utcnow()
                ))
            
            if rows:
                conn = psycopg2.connect(
                    host="postgres", 
                    dbname="city_mood", 
                    user="spark", 
                    password="spark"
                )
                conn.autocommit = True
                
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS city_mood_scores (
                            window_start TIMESTAMP NOT NULL,
                            window_end TIMESTAMP NOT NULL,
                            city_mood_score DOUBLE PRECISION NOT NULL,
                            news_score DOUBLE PRECISION,
                            air_score DOUBLE PRECISION,
                            weather_score DOUBLE PRECISION,
                            traffic_score DOUBLE PRECISION,
                            alerts_score DOUBLE PRECISION,
                            construction_score DOUBLE PRECISION,
                            water_score DOUBLE PRECISION,
                            data_points_count INTEGER,
                            updated_at TIMESTAMP NOT NULL,
                            PRIMARY KEY (window_start)
                        );
                    """)
                    
                    sql = """
                        INSERT INTO city_mood_scores 
                        (window_start, window_end, city_mood_score, news_score, air_score, 
                         weather_score, traffic_score, alerts_score, construction_score, 
                         water_score, data_points_count, updated_at)
                        VALUES %s
                        ON CONFLICT (window_start) 
                        DO UPDATE SET 
                            city_mood_score = EXCLUDED.city_mood_score,
                            news_score = EXCLUDED.news_score,
                            air_score = EXCLUDED.air_score,
                            weather_score = EXCLUDED.weather_score,
                            traffic_score = EXCLUDED.traffic_score,
                            alerts_score = EXCLUDED.alerts_score,
                            construction_score = EXCLUDED.construction_score,
                            water_score = EXCLUDED.water_score,
                            data_points_count = EXCLUDED.data_points_count,
                            updated_at = EXCLUDED.updated_at;
                    """
                    execute_values(cur, sql, rows)
                    logger.info(f"✅ {len(rows)} Zeilen in Postgres geschrieben")
                
                conn.close()
        else:
            logger.error(f"❌ Batch {batch_id} hat kritische Issues - NICHT in DB geschrieben")
    
    except Exception as e:
        logger.error(f"❌ Fehler in Batch {batch_id}: {str(e)}", exc_info=True)

# 8. Start Stream

import pandas as pd

logger.info("🚀 Starting City Mood Score Stream...")

query_city_mood = (
    city_mood_final.writeStream
    .outputMode("append")
    .foreachBatch(write_city_mood_to_postgres)
   # .trigger(processingTime="30 seconds")
   .trigger(processingTime="1 minute")
    .option("checkpointLocation","/tmp/city_mood_once")
    .start()
)

logger.info("Started City Mood Score Stream.")
logger.info("Reports are generated in: /app/gx-reports/")
logger.info("Data is written to: PostgreSQL (city_mood_scores)")

query_city_mood.awaitTermination()