import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, avg, count, window, current_timestamp,
    when, coalesce, lit, least, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import pandas as pd
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.validator.validator import Validator
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch, BatchMarkers

import os
import shutil
import glob
import traceback
from pathlib import Path

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# SPARK SESSION CONFIGURATION
spark = SparkSession.builder \
    .appName("CityMoodRealtimeStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", 
            "org.postgresql:postgresql:42.7.8,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.executor.memory", "10g") \
    .config("spark.executor.cores", "6") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# CONFIGURATION
KAFKA_BOOTSTRAP = "kafka:9092"
CHECKPOINT_DIR = "/tmp/spark-checkpoint/city-mood-union-modev7"
WINDOW_DURATION = "1 hour"
WATERMARK_DELAY = "2 minutes"
TRIGGER_INTERVAL = "20 seconds"

PG_CONFIG = {
    "host": "postgres",
    "dbname": "city_mood",
    "user": "spark",
    "password": "spark"
}

logger.info("=" * 80)
logger.info("CITY MOOD REAL-TIME STREAMING PIPELINE - GREAT EXPECTATIONS")
logger.info("=" * 80)
logger.info(f"Trigger Interval: {TRIGGER_INTERVAL}")
logger.info(f"Window Duration: {WINDOW_DURATION}")
logger.info(f"Watermark Delay: {WATERMARK_DELAY}")
logger.info(f"Output Mode: UPDATE (union-based aggregation)")
logger.info(f"Data Quality: Great Expectations")
logger.info("=" * 80)

# GREAT EXPECTATIONS SETUP
os.makedirs("./gx-reports", exist_ok=True)

# Create GX Context (for v1.0+)
context = gx.get_context()

logger.info("Great Expectations context initialized")

# HTML REPORT MANAGEMENT
HTML_REPORT_PATH = "./gx-reports/validation_report.html"

def initialize_html_report():
    """Initialize the HTML report file with header and table structure"""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Great Expectations Validation Report - City Mood Pipeline</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 28px;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .header p {
            font-size: 14px;
            opacity: 0.9;
        }
        
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
            border-bottom: 2px solid #e9ecef;
        }
        
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            text-align: center;
        }
        
        .stat-value {
            font-size: 32px;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }
        
        .stat-label {
            font-size: 12px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .table-container {
            padding: 30px;
            overflow-x: auto;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
        }
        
        thead {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        th {
            padding: 15px 12px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        td {
            padding: 12px;
            border-bottom: 1px solid #e9ecef;
            font-size: 13px;
        }
        
        tbody tr:hover {
            background: #f8f9fa;
            transition: background 0.3s ease;
        }
        
        .badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .badge-success {
            background: #d4edda;
            color: #155724;
        }
        
        .badge-danger {
            background: #f8d7da;
            color: #721c24;
        }
        
        .badge-warning {
            background: #fff3cd;
            color: #856404;
        }
        
        .score {
            font-weight: bold;
            font-size: 14px;
        }
        
        .score-high {
            color: #28a745;
        }
        
        .score-medium {
            color: #ffc107;
        }
        
        .score-low {
            color: #dc3545;
        }
        
        .timestamp {
            color: #6c757d;
            font-size: 12px;
        }
        
        .details {
            font-size: 11px;
            color: #6c757d;
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        
        .footer {
            text-align: center;
            padding: 20px;
            background: #f8f9fa;
            color: #6c757d;
            font-size: 12px;
            border-top: 1px solid #e9ecef;
        }
        
        .empty-state {
            text-align: center;
            padding: 60px 30px;
            color: #6c757d;
        }
        
        .empty-state i {
            font-size: 64px;
            margin-bottom: 20px;
            opacity: 0.3;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Great Expectations Validation Report</h1>
            <p>City Mood Real-Time Streaming Pipeline</p>
        </div>
        
        <div class="stats" id="stats">
            <div class="stat-card">
                <div class="stat-value" id="total-batches">0</div>
                <div class="stat-label">Total Batches</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="passed-batches">0</div>
                <div class="stat-label">Passed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="failed-batches">0</div>
                <div class="stat-label">Failed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="success-rate">0%</div>
                <div class="stat-label">Success Rate</div>
            </div>
        </div>
        
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Batch ID</th>
                        <th>Timestamp</th>
                        <th>Status</th>
                        <th>Success %</th>
                        <th>Records</th>
                        <th>Avg Mood</th>
                        <th>Data Points</th>
                        <th>Failed Expectations</th>
                        <th>Warnings</th>
                    </tr>
                </thead>
                <tbody id="validation-body">
                    <!-- Validation results will be inserted here -->
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>Last updated: <span id="last-update">Never</span></p>
            <p>Generated by City Mood Streaming Pipeline with Great Expectations</p>
        </div>
    </div>
    
    <script>
        function updateStats() {
            const rows = document.querySelectorAll('#validation-body tr');
            const total = rows.length;
            let passed = 0;
            let failed = 0;
            
            rows.forEach(row => {
                const status = row.querySelector('.badge');
                if (status && status.classList.contains('badge-success')) {
                    passed++;
                } else if (status && status.classList.contains('badge-danger')) {
                    failed++;
                }
            });
            
            const successRate = total > 0 ? ((passed / total) * 100).toFixed(1) : 0;
            
            document.getElementById('total-batches').textContent = total;
            document.getElementById('passed-batches').textContent = passed;
            document.getElementById('failed-batches').textContent = failed;
            document.getElementById('success-rate').textContent = successRate + '%';
        }
        
        // Update stats on page load
        updateStats();
    </script>
</body>
</html>
"""
    
    with open(HTML_REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"HTML report initialized: {HTML_REPORT_PATH}")


def append_validation_to_html(batch_id, validation_result, batch_stats):
    """Append a new validation result to the HTML report"""
    try:

        with open(HTML_REPORT_PATH, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Determine status
        success = validation_result.get("success", False)
        failed_count = len(validation_result.get("failed_expectations", []))
        warnings_count = len(validation_result.get("warnings", []))
        
        if success:
            status_badge = '<span class="badge badge-success">✓ Passed</span>'
        elif failed_count > 0:
            status_badge = '<span class="badge badge-danger">✗ Failed</span>'
        else:
            status_badge = '<span class="badge badge-warning">⚠ Warning</span>'
        
        # Get success percentage
        stats = validation_result.get("statistics", {})
        success_percent = stats.get("success_percent", 0)
        
        # Color code for success percentage
        if success_percent >= 95:
            score_class = "score-high"
        elif success_percent >= 80:
            score_class = "score-medium"
        else:
            score_class = "score-low"
        
        # Batch statistics
        num_records = batch_stats.get("num_records", 0)
        avg_mood = batch_stats.get("avg_mood", 0)
        total_points = batch_stats.get("total_points", 0)
        time_range = batch_stats.get("time_range", "N/A")
        
        # Color code for mood score
        if avg_mood >= 0.7:
            mood_class = "score-high"
        elif avg_mood >= 0.5:
            mood_class = "score-medium"
        else:
            mood_class = "score-low"
        
        # Format failed expectations
        failed_exp_text = ""
        if failed_count > 0:
            failed_exp = validation_result.get("failed_expectations", [])
            failed_exp_text = "; ".join(failed_exp[:3])  # Show first 3
            if failed_count > 3:
                failed_exp_text += f" ... (+{failed_count - 3} more)"
        else:
            failed_exp_text = "-"
        
        # Format warnings
        warnings_text = ""
        if warnings_count > 0:
            warnings = validation_result.get("warnings", [])
            warnings_text = "; ".join(warnings[:2])  # Show first 2
            if warnings_count > 2:
                warnings_text += f" ... (+{warnings_count - 2} more)"
        else:
            warnings_text = "-"
        
        # Current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Create new row
        new_row = f"""
                    <tr>
                        <td><strong>#{batch_id}</strong></td>
                        <td class="timestamp">{timestamp}<br><small>{time_range}</small></td>
                        <td>{status_badge}</td>
                        <td><span class="score {score_class}">{success_percent:.1f}%</span></td>
                        <td>{num_records}</td>
                        <td><span class="score {mood_class}">{avg_mood:.3f}</span></td>
                        <td>{total_points}</td>
                        <td><div class="details" title="{failed_exp_text}">{failed_exp_text}</div></td>
                        <td><div class="details" title="{warnings_text}">{warnings_text}</div></td>
                    </tr>"""
        
        # Insert new row at the beginning of tbody (newest first)
        tbody_marker = '<tbody id="validation-body">'
        insert_pos = html_content.find(tbody_marker) + len(tbody_marker)
        
        html_content = (
            html_content[:insert_pos] + 
            new_row + 
            html_content[insert_pos:]
        )
        
        # Update last update timestamp
        html_content = html_content.replace(
            '<span id="last-update">Never</span>',
            f'<span id="last-update">{timestamp}</span>'
        )
        
        # Write back to file
        with open(HTML_REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Batch {batch_id}: Validation result added to HTML report")
        
    except Exception as e:
        logger.error(f"Error updating HTML report: {e}")
        logger.error(traceback.format_exc())


# Initialize HTML report at startup
initialize_html_report()

def create_expectation_suite():
    """
    Ensure an expectation suite exists for our pipeline.
    For Great Expectations >= 1.0 we create an empty suite (if missing)
    and use the validator to populate expectations at validation time.
    """
    suite_name = "city_mood_quality_suite"

    try:
        suite = context.suites.get(name=suite_name)
        logger.info(f"Using existing Expectation Suite: {suite_name}")
        return suite
    except Exception:
        pass

    logger.info(f"Creating new Expectation Suite: {suite_name}")

    # Create an empty ExpectationSuite object and add to context
    suite = ExpectationSuite(name=suite_name)
    try:
        context.suites.add(suite)
    except Exception:
        # fallback: save
        try:
            context.suites.save(suite)
        except Exception as e:
            logger.warning(f"Could not persist new suite via context.suites.add/save: {e}")

    logger.info(f"Empty Expectation Suite created: {suite_name}")
    return suite

def apply_expectations(validator):
    """
    Apply the expectations to the validator (Great Expectations >=1.0 style).
    Keep critical expectations first.
    """
    # --- CRITICAL: Main Score Validation ---
    validator.expect_column_to_exist("city_mood_score")
    validator.expect_column_values_to_not_be_null("city_mood_score")
    validator.expect_column_values_to_be_between(
        "city_mood_score", min_value=0.0, max_value=1.0, mostly=1.0
    )

    # --- Component Scores Validation ---
    validator.expect_column_values_to_be_between("news_score", min_value=0.0, max_value=1.0, mostly=1.0)
    validator.expect_column_values_to_be_between("air_score", min_value=0.0, max_value=1.0, mostly=1.0)
    validator.expect_column_values_to_be_between("weather_score", min_value=0.0, max_value=1.0, mostly=1.0)
    validator.expect_column_values_to_be_between("traffic_score", min_value=0.0, max_value=1.0, mostly=1.0)
    validator.expect_column_values_to_be_between("alerts_score", min_value=0.0, max_value=1.0, mostly=1.0)
    validator.expect_column_values_to_be_between("construction_score", min_value=0.0, max_value=1.0, mostly=1.0)
    validator.expect_column_values_to_be_between("water_score", min_value=0.0, max_value=1.0, mostly=1.0)

    # --- Timestamp Validation ---
    validator.expect_column_to_exist("window_start")
    validator.expect_column_values_to_not_be_null("window_start")
    validator.expect_column_to_exist("window_end")
    validator.expect_column_values_to_not_be_null("window_end")

    # --- Duplicate Check ---
    # For streaming/windowed data unique window_start may be desired
    try:
        validator.expect_column_values_to_be_unique("window_start")
    except Exception:
        # some GE installations enforce different semantics; ignore if not available
        logger.debug("expect_column_values_to_be_unique not available or not applicable in this context")

    # --- Plausibility Checks ---
    validator.expect_column_values_to_be_between("avg_temp", min_value=-30.0, max_value=50.0, mostly=0.95)
    validator.expect_column_values_to_be_between("avg_aqi", min_value=0.0, max_value=500.0, mostly=0.95)
    validator.expect_column_values_to_be_between("avg_water_level", min_value=0.0, max_value=2000.0, mostly=0.95)

    # --- Data Availability Checks (use min_value only to avoid None issues) ---
    validator.expect_column_mean_to_be_between("news_count", min_value=0.5)
    validator.expect_column_mean_to_be_between("traffic_count", min_value=3.0)
    validator.expect_column_mean_to_be_between("total_data_points", min_value=5.0)

    # --- Schema Validation ---
    validator.expect_table_columns_to_match_set(
        column_set=[
            "window_start", "window_end", "city_mood_score",
            "news_score", "air_score", "weather_score", "traffic_score",
            "alerts_score", "construction_score", "water_score",
            "news_count", "air_count", "weather_count", "traffic_count",
            "alert_count", "construction_count", "water_count",
            "total_data_points", "avg_aqi", "avg_temp", "avg_water_level",
            "computed_at"
        ]
    )

    # Save expectation suite back to the context (persist)
    try:
        validator.save_expectation_suite()
    except Exception as e:
        # Some GX backends may require alternate saving; log and continue
        logger.warning(f"Could not save expectation suite via validator.save_expectation_suite(): {e}")

# Create expectation suite at startup (ensures suite exists)
expectation_suite = create_expectation_suite()

# ============================================================================
# SCHEMAS
# ============================================================================
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

air_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("current", StructType([
        StructField("european_aqi", IntegerType(), True),
        StructField("pm2_5", DoubleType(), True),
        StructField("pm10", DoubleType(), True)
    ]), True)
])

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

traffic_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("properties", StructType([
        StructField("zustandsklasse", StringType(), True),
        StructField("strassenklasse", StringType(), True)
    ]), True)
])

alerts_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("alert", StructType([
        StructField("severity", StringType(), True),
        StructField("valid", BooleanType(), True),
        StructField("headline", StringType(), True)
    ]), True)
])

construction_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("properties", StructType([
        StructField("iststoerung", BooleanType(), True),
        StructField("titel", StringType(), True)
    ]), True)
])

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

# ============================================================================
# UNIFIED KAFKA STREAM READER
# ============================================================================
def create_unified_stream():
    """Create a single unified stream from all Kafka topics"""
    logger.info("Creating unified Kafka stream...")
    
    all_topics = [
        "bbc-europe-news",
        "nyt-europe-news", 
        "nyt-world-news",
        "hh-air-pollution-current",
        "hh-weather-current",
        "hh-traffic-data",
        "hh-public-alerts-current",
        "hh-street-construction",
        "hh-water-level-current"
    ]
    
    logger.info("Starting from EARLIEST offsets")
    
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", ",".join(all_topics)) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "5000") \
        .load() \
        .select(
            col("topic"),
            col("value").cast("string").alias("json_value"),
            col("timestamp").alias("kafka_timestamp")
        )

# ============================================================================
# PARSE AND UNIFY ALL DATA
# ============================================================================

def parse_and_unify_all(df):
    """Parse all topics and unify into single stream for aggregation"""
    
    logger.info("Parsing and unifying all data streams...")
    
    # ========== NEWS ==========
    news_df = df.filter(col("topic").isin("bbc-europe-news", "nyt-europe-news", "nyt-world-news")) \
        .select(
            from_json(col("json_value"), news_schema).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .withColumn("news_score",
            when(col("headline_sentiment.label") == "POSITIVE", col("headline_sentiment.score"))
            .when(col("headline_sentiment.label") == "NEUTRAL", lit(0.5))
            .otherwise(lit(1.0) - col("headline_sentiment.score"))
        ) \
        .select(
            col("event_time"),
            col("news_score"),
            lit(None).cast("double").alias("air_score"),
            lit(None).cast("double").alias("weather_score"),
            lit(None).cast("double").alias("traffic_score"),
            lit(None).cast("double").alias("alert_score"),
            lit(None).cast("integer").alias("alert_count_raw"),
            lit(None).cast("double").alias("avg_alert_impact"),
            lit(None).cast("integer").alias("construction_count_raw"),
            lit(None).cast("double").alias("water_score"),
            lit(None).cast("double").alias("aqi"),
            lit(None).cast("double").alias("temperature"),
            lit(None).cast("double").alias("water_level")
        )
    
    # ========== AIR QUALITY ==========
    air_df = df.filter(col("topic") == "hh-air-pollution-current") \
        .select(
            from_json(col("json_value"), air_schema).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .withColumn("air_score",
            when(col("current.european_aqi") <= 50, lit(1.0))
            .when(col("current.european_aqi") <= 100,
                  lit(0.7) - ((col("current.european_aqi") - 50) * 0.006))
            .when(col("current.european_aqi") <= 150,
                  lit(0.4) - ((col("current.european_aqi") - 100) * 0.006))
            .otherwise(lit(0.1))
        ) \
        .select(
            col("event_time"),
            lit(None).cast("double").alias("news_score"),
            col("air_score"),
            lit(None).cast("double").alias("weather_score"),
            lit(None).cast("double").alias("traffic_score"),
            lit(None).cast("double").alias("alert_score"),
            lit(None).cast("integer").alias("alert_count_raw"),
            lit(None).cast("double").alias("avg_alert_impact"),
            lit(None).cast("integer").alias("construction_count_raw"),
            lit(None).cast("double").alias("water_score"),
            col("current.european_aqi").cast("double").alias("aqi"),
            lit(None).cast("double").alias("temperature"),
            lit(None).cast("double").alias("water_level")
        )
    
    # ========== WEATHER ==========
    weather_df = df.filter(col("topic") == "hh-weather-current") \
        .select(
            from_json(col("json_value"), weather_schema).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .withColumn("temp_score",
            when((col("current.temperature_2m") >= 15) & (col("current.temperature_2m") <= 25), lit(1.0))
            .when((col("current.temperature_2m") >= 10) & (col("current.temperature_2m") < 15), lit(0.8))
            .when((col("current.temperature_2m") > 25) & (col("current.temperature_2m") <= 30), lit(0.8))
            .when((col("current.temperature_2m") >= 5) & (col("current.temperature_2m") < 10), lit(0.6))
            .when((col("current.temperature_2m") >= -5) & (col("current.temperature_2m") < 5), lit(0.4))
            .otherwise(lit(0.2))
        ) \
        .withColumn("weather_code_score",
            when(col("current.weather_code") <= 1, lit(1.0))
            .when(col("current.weather_code") <= 3, lit(0.8))
            .when(col("current.weather_code") <= 48, lit(0.6))
            .when(col("current.weather_code") <= 67, lit(0.5))
            .when(col("current.weather_code") <= 77, lit(0.4))
            .otherwise(lit(0.2))
        ) \
        .withColumn("precip_score",
            when(col("current.precipitation") == 0, lit(1.0))
            .when(col("current.precipitation") < 2, lit(0.8))
            .when(col("current.precipitation") < 5, lit(0.6))
            .otherwise(lit(0.4))
        ) \
        .withColumn("wind_score",
            when(col("current.wind_speed_10m") < 15, lit(1.0))
            .when(col("current.wind_speed_10m") < 25, lit(0.7))
            .otherwise(lit(0.4))
        ) \
        .withColumn("weather_score",
            col("temp_score") * 0.4 + col("weather_code_score") * 0.3 + 
            col("precip_score") * 0.2 + col("wind_score") * 0.1
        ) \
        .select(
            col("event_time"),
            lit(None).cast("double").alias("news_score"),
            lit(None).cast("double").alias("air_score"),
            col("weather_score"),
            lit(None).cast("double").alias("traffic_score"),
            lit(None).cast("double").alias("alert_score"),
            lit(None).cast("integer").alias("alert_count_raw"),
            lit(None).cast("double").alias("avg_alert_impact"),
            lit(None).cast("integer").alias("construction_count_raw"),
            lit(None).cast("double").alias("water_score"),
            lit(None).cast("double").alias("aqi"),
            col("current.temperature_2m").cast("double").alias("temperature"),
            lit(None).cast("double").alias("water_level")
        )
    
    # ========== TRAFFIC ==========
    traffic_df = df.filter(col("topic") == "hh-traffic-data") \
        .select(
            from_json(col("json_value"), traffic_schema).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .withColumn("traffic_score",
            when(col("properties.zustandsklasse") == "fliessend", lit(1.0))
            .when(col("properties.zustandsklasse") == "zaehfliessend", lit(0.6))
            .when(col("properties.zustandsklasse") == "stockend", lit(0.3))
            .otherwise(lit(0.1))
        ) \
        .select(
            col("event_time"),
            lit(None).cast("double").alias("news_score"),
            lit(None).cast("double").alias("air_score"),
            lit(None).cast("double").alias("weather_score"),
            col("traffic_score"),
            lit(None).cast("double").alias("alert_score"),
            lit(None).cast("integer").alias("alert_count_raw"),
            lit(None).cast("double").alias("avg_alert_impact"),
            lit(None).cast("integer").alias("construction_count_raw"),
            lit(None).cast("double").alias("water_score"),
            lit(None).cast("double").alias("aqi"),
            lit(None).cast("double").alias("temperature"),
            lit(None).cast("double").alias("water_level")
        )
    
    # ========== ALERTS ==========
    alerts_df = df.filter(col("topic") == "hh-public-alerts-current") \
        .select(
            from_json(col("json_value"), alerts_schema).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .filter(col("alert.valid") == True) \
        .withColumn("alert_score",
            when(col("alert.severity") == "Minor", lit(0.9))
            .when(col("alert.severity") == "Moderate", lit(0.7))
            .when(col("alert.severity") == "Severe", lit(0.4))
            .otherwise(lit(0.1))
        ) \
        .select(
            col("event_time"),
            lit(None).cast("double").alias("news_score"),
            lit(None).cast("double").alias("air_score"),
            lit(None).cast("double").alias("weather_score"),
            lit(None).cast("double").alias("traffic_score"),
            col("alert_score"),
            lit(1).cast("integer").alias("alert_count_raw"),
            col("alert_score").alias("avg_alert_impact"),
            lit(None).cast("integer").alias("construction_count_raw"),
            lit(None).cast("double").alias("water_score"),
            lit(None).cast("double").alias("aqi"),
            lit(None).cast("double").alias("temperature"),
            lit(None).cast("double").alias("water_level")
        )
    
    # ========== CONSTRUCTION ==========
    construction_df = df.filter(col("topic") == "hh-street-construction") \
        .select(
            from_json(col("json_value"), construction_schema).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .filter(col("properties.iststoerung") == True) \
        .select(
            col("event_time"),
            lit(None).cast("double").alias("news_score"),
            lit(None).cast("double").alias("air_score"),
            lit(None).cast("double").alias("weather_score"),
            lit(None).cast("double").alias("traffic_score"),
            lit(None).cast("double").alias("alert_score"),
            lit(None).cast("integer").alias("alert_count_raw"),
            lit(None).cast("double").alias("avg_alert_impact"),
            lit(1).cast("integer").alias("construction_count_raw"),
            lit(None).cast("double").alias("water_score"),
            lit(None).cast("double").alias("aqi"),
            lit(None).cast("double").alias("temperature"),
            lit(None).cast("double").alias("water_level")
        )
    
    # ========== WATER ==========
    water_df = df.filter(col("topic") == "hh-water-level-current") \
        .select(
            from_json(col("json_value"), water_schema).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("fetch_timestamp"))) \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .filter(col("station.water") == "ELBE") \
        .withColumn("water_score",
            when((col("measurement.value_cm") >= 400) & (col("measurement.value_cm") <= 700), lit(1.0))
            .when((col("measurement.value_cm") > 700) & (col("measurement.value_cm") <= 800), lit(0.7))
            .when(col("measurement.value_cm") > 800, lit(0.3))
            .when((col("measurement.value_cm") >= 300) & (col("measurement.value_cm") < 400), lit(0.8))
            .otherwise(lit(0.5))
        ) \
        .select(
            col("event_time"),
            lit(None).cast("double").alias("news_score"),
            lit(None).cast("double").alias("air_score"),
            lit(None).cast("double").alias("weather_score"),
            lit(None).cast("double").alias("traffic_score"),
            lit(None).cast("double").alias("alert_score"),
            lit(None).cast("integer").alias("alert_count_raw"),
            lit(None).cast("double").alias("avg_alert_impact"),
            lit(None).cast("integer").alias("construction_count_raw"),
            col("water_score"),
            lit(None).cast("double").alias("aqi"),
            lit(None).cast("double").alias("temperature"),
            col("measurement.value_cm").cast("double").alias("water_level")
        )
    
    # ========== UNION ALL STREAMS ==========
    logger.info("Unifying all data streams with unionByName...")
    
    unified = news_df \
        .unionByName(air_df) \
        .unionByName(weather_df) \
        .unionByName(traffic_df) \
        .unionByName(alerts_df) \
        .unionByName(construction_df) \
        .unionByName(water_df)
    
    # ========== SINGLE AGGREGATION ==========
    logger.info("Creating single aggregation over unified stream...")
    
    aggregated = unified.groupBy(window("event_time", WINDOW_DURATION)) \
        .agg(
            # Score averages
            avg("news_score").alias("news_score"),
            avg("air_score").alias("air_score"),
            avg("weather_score").alias("weather_score"),
            avg("traffic_score").alias("traffic_score"),
            avg("water_score").alias("water_score"),
            
            # Alert score calculation
            count(when(col("alert_count_raw").isNotNull(), 1)).alias("alert_count"),
            avg("avg_alert_impact").alias("avg_alert_impact"),
            
            # Construction score calculation
            count(when(col("construction_count_raw").isNotNull(), 1)).alias("construction_count"),
            
            # Metrics
            avg("aqi").alias("avg_aqi"),
            avg("temperature").alias("avg_temp"),
            avg("water_level").alias("avg_water_level"),
            
            # Counts
            count(when(col("news_score").isNotNull(), 1)).alias("news_count"),
            count(when(col("air_score").isNotNull(), 1)).alias("air_count"),
            count(when(col("weather_score").isNotNull(), 1)).alias("weather_count"),
            count(when(col("traffic_score").isNotNull(), 1)).alias("traffic_count"),
            count(when(col("water_score").isNotNull(), 1)).alias("water_count")
        ) \
        .withColumn("alerts_score",
            when(col("alert_count") == 0, lit(1.0))
            .otherwise(
                col("avg_alert_impact") * (lit(1.0) - least(col("alert_count") * 0.1, lit(0.3)))
            )
        ) \
        .withColumn("construction_score",
            when(col("construction_count") == 0, lit(1.0))
            .when(col("construction_count") <= 5, lit(0.9))
            .when(col("construction_count") <= 15, lit(0.7))
            .otherwise(lit(0.5))
        )
    
    return aggregated

# ============================================================================
# CREATE STREAM AND CALCULATE FINAL SCORES
# ============================================================================
logger.info("Setting up streaming pipeline...")

unified_stream = create_unified_stream()
city_mood = parse_and_unify_all(unified_stream)

# Calculate final score
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
    coalesce(col("news_count"), lit(0)).alias("news_count"),
    coalesce(col("air_count"), lit(0)).alias("air_count"),
    coalesce(col("weather_count"), lit(0)).alias("weather_count"),
    coalesce(col("traffic_count"), lit(0)).alias("traffic_count"),
    coalesce(col("alert_count"), lit(0)).alias("alert_count"),
    coalesce(col("construction_count"), lit(0)).alias("construction_count"),
    coalesce(col("water_count"), lit(0)).alias("water_count"),
    col("avg_aqi"),
    col("avg_temp"),
    col("avg_water_level")
).withColumn(
    "city_mood_score",
    (coalesce(col("news_score"), lit(0.5)) * 0.25 +
     coalesce(col("air_score"), lit(0.7)) * 0.15 +
     coalesce(col("weather_score"), lit(0.6)) * 0.20 +
     coalesce(col("traffic_score"), lit(0.8)) * 0.15 +
     coalesce(col("alerts_score"), lit(1.0)) * 0.15 +
     coalesce(col("construction_score"), lit(0.8)) * 0.05 +
     coalesce(col("water_score"), lit(1.0)) * 0.05)
).withColumn(
    "total_data_points",
    col("news_count") + col("air_count") + col("weather_count") + 
    col("traffic_count") + col("alert_count") + col("construction_count") + 
    col("water_count")
).withColumn(
    "computed_at", current_timestamp()
)

# ============================================================================
# GREAT EXPECTATIONS VALIDATION
# ============================================================================
def validate_with_great_expectations(pdf, batch_id):
    """
    Validate data quality using Great Expectations with EphemeralDataContext.
    Uses Validator with PandasExecutionEngine.
    """
    logger.info(f"Batch {batch_id}: Running Great Expectations validation...")

    try:
        # Create Batch object for the DataFrame
        ge_load_time = datetime.utcnow().isoformat()
        batch = Batch(
            data=pdf,
            batch_markers=BatchMarkers(ge_load_time=ge_load_time)
        )

        # Create validator directly from pandas DataFrame
        validator = Validator(
            execution_engine=PandasExecutionEngine(batch_data_dict={"batch": pdf}),
            batches=[batch],
            expectation_suite=expectation_suite
        )

        # Apply expectations
        apply_expectations(validator)

        # Run validation
        validation_result = validator.validate()

        success = validation_result.success
        statistics = validation_result.statistics

        failed_expectations = []
        warnings = []

        for result in validation_result.results:
            if not result.success:
                exp_type = result.expectation_config.type
                column = result.expectation_config.kwargs.get("column", "N/A")

                if exp_type in [
                    "expect_column_values_to_not_be_null",
                    "expect_column_values_to_be_between"
                ] and column in [
                    "city_mood_score", "news_score", "air_score",
                    "weather_score", "traffic_score"
                ]:
                    failed_expectations.append(f"❌ {exp_type} failed for {column}")
                else:
                    warnings.append(f"⚠️ {exp_type} failed for {column}")

        # Logging
        if success:
            logger.info(
                f"   ✅ Batch {batch_id}: Validation PASSED "
                f"({statistics.get('success_percent', 0):.1f}%)"
            )
        else:
            logger.warning(
                f"   ⚠️ Batch {batch_id}: Validation FAILED "
                f"({statistics.get('success_percent', 0):.1f}%)"
            )

        for f in failed_expectations:
            logger.error(f"      {f}")
        for w in warnings:
            logger.warning(f"      {w}")

        return {
            "success": success,
            "statistics": statistics,
            "failed_expectations": failed_expectations,
            "warnings": warnings
        }

    except Exception as e:
        logger.error(f"   ❌ Batch {batch_id}: Great Expectations validation error: {e}")
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e),
            "failed_expectations": [f"❌ Validation crashed: {e}"],
            "warnings": []
        }

# ============================================================================
# POSTGRES WRITER WITH GREAT EXPECTATIONS
# ============================================================================
def write_to_postgres(batch_df, batch_id):
    """Write batch to PostgreSQL with Great Expectations validation"""
    
    # Spark DataFrame API check
    try:
        if hasattr(batch_df, "isEmpty") and batch_df.isEmpty():
            logger.info(f"⏭️  Batch {batch_id}: Empty batch, skipping")
            return
    except Exception:
        # Some Spark versions may not expose isEmpty in foreachBatch; fallback to count
        if batch_df.count() == 0:
            logger.info(f"⏭️  Batch {batch_id}: Empty batch, skipping")
            return

    batch_count = batch_df.count()
    logger.info(f"💾 Batch {batch_id}: Processing {batch_count} records")
    
    pdf = batch_df.toPandas()
    
    # Prepare batch statistics for HTML report
    batch_stats = {
        "num_records": len(pdf),
        "avg_mood": 0,
        "total_points": 0,
        "time_range": "N/A"
    }
    
    # Log batch statistics
    if len(pdf) > 0:
        try:
            batch_stats["avg_mood"] = float(pdf['city_mood_score'].mean())
            batch_stats["total_points"] = int(pdf['total_data_points'].sum())
            batch_stats["time_range"] = f"{pdf['window_start'].min()} - {pdf['window_end'].max()}"
            
            logger.info(f"   📊 Batch {batch_id} Stats:")
            logger.info(f"      Time range: {batch_stats['time_range']}")
            logger.info(f"      Avg City Mood: {batch_stats['avg_mood']:.3f}")
            logger.info(f"      Total data points: {batch_stats['total_points']}")
        except Exception as e:
            logger.debug(f"Could not compute some batch stats for log: {e}")
    
    # GREAT EXPECTATIONS VALIDATION
    validation_result = validate_with_great_expectations(pdf, batch_id)
    
    # ADD TO HTML REPORT
    append_validation_to_html(batch_id, validation_result, batch_stats)

    # Only write to database if validation passed or only warnings present
    if not validation_result.get("success", False) and len(validation_result.get("failed_expectations", [])) > 0:
        logger.error(f"   ❌ Batch {batch_id}: Critical validation errors - SKIPPING database write")
        logger.error(f"      Failed expectations: {len(validation_result.get('failed_expectations', []))}")
        return
    
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    
    try:
        with conn.cursor() as cur:
            # Create table if not exists
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
                    total_data_points INTEGER,
                    avg_aqi DOUBLE PRECISION,
                    avg_temp DOUBLE PRECISION,
                    avg_water_level DOUBLE PRECISION,
                    computed_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (window_start)
                );
            """)
            
            # Add missing columns if they don't exist (schema migration)
            cur.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='city_mood_scores' AND column_name='total_data_points'
                    ) THEN
                        ALTER TABLE city_mood_scores ADD COLUMN total_data_points INTEGER;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='city_mood_scores' AND column_name='avg_aqi'
                    ) THEN
                        ALTER TABLE city_mood_scores ADD COLUMN avg_aqi DOUBLE PRECISION;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='city_mood_scores' AND column_name='avg_temp'
                    ) THEN
                        ALTER TABLE city_mood_scores ADD COLUMN avg_temp DOUBLE PRECISION;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='city_mood_scores' AND column_name='avg_water_level'
                    ) THEN
                        ALTER TABLE city_mood_scores ADD COLUMN avg_water_level DOUBLE PRECISION;
                    END IF;
                END $$;
            """)
            
            # Create indexes
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_window_start ON city_mood_scores(window_start DESC);
                CREATE INDEX IF NOT EXISTS idx_city_mood_score ON city_mood_scores(city_mood_score);
            """)
            
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
                    int(row["total_data_points"]) if not pd.isna(row["total_data_points"]) else 0,
                    float(row["avg_aqi"]) if not pd.isna(row["avg_aqi"]) else None,
                    float(row["avg_temp"]) if not pd.isna(row["avg_temp"]) else None,
                    float(row["avg_water_level"]) if not pd.isna(row["avg_water_level"]) else None,
                    row["computed_at"],
                    datetime.now(timezone.utc)
                ))
            
            sql = """
                INSERT INTO city_mood_scores 
                (window_start, window_end, city_mood_score, news_score, air_score, 
                 weather_score, traffic_score, alerts_score, construction_score, 
                 water_score, total_data_points, avg_aqi, avg_temp, avg_water_level,
                 computed_at, updated_at)
                VALUES %s
                ON CONFLICT (window_start) 
                DO UPDATE SET 
                    window_end = EXCLUDED.window_end,
                    city_mood_score = EXCLUDED.city_mood_score,
                    news_score = EXCLUDED.news_score,
                    air_score = EXCLUDED.air_score,
                    weather_score = EXCLUDED.weather_score,
                    traffic_score = EXCLUDED.traffic_score,
                    alerts_score = EXCLUDED.alerts_score,
                    construction_score = EXCLUDED.construction_score,
                    water_score = EXCLUDED.water_score,
                    total_data_points = EXCLUDED.total_data_points,
                    avg_aqi = EXCLUDED.avg_aqi,
                    avg_temp = EXCLUDED.avg_temp,
                    avg_water_level = EXCLUDED.avg_water_level,
                    computed_at = EXCLUDED.computed_at,
                    updated_at = EXCLUDED.updated_at;
            """
            
            execute_values(cur, sql, rows, page_size=1000)
            logger.info(f"   ✅ Batch {batch_id}: Successfully written {len(rows)} records")
            
    except Exception as e:
        logger.error(f"   ❌ Batch {batch_id}: Database error: {e}")
        raise
    finally:
        conn.close()

# ============================================================================
# START STREAMING QUERY
# ============================================================================
logger.info("Starting streaming query...")

query = city_mood_final \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

logger.info("=" * 80)
logger.info("PIPELINE IS RUNNING - WITH GREAT EXPECTATIONS VALIDATION")
logger.info("=" * 80)
logger.info(f"Reading from all Kafka topics")
logger.info(f"Triggering every {TRIGGER_INTERVAL}")
logger.info(f"Window: {WINDOW_DURATION}")
logger.info(f"Watermark: {WATERMARK_DELAY}")
logger.info(f"Database: {PG_CONFIG['host']}/{PG_CONFIG['dbname']}")
logger.info("=" * 80)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Stopping pipeline gracefully...")
    query.stop()
    spark.stop()
    logger.info("Pipeline stopped")