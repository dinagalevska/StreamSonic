import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pyspark.sql.window import Window

def table_exists(path: str) -> bool:
    return os.path.exists(path)

APP_NAME = os.getenv("APP_NAME", "Session Dimension")
TEMP_GCS_BUCKET = os.getenv("TEMP_GCS_BUCKET", "streamsonic_bucket")

spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("fs.gs.auth.service.account.enable", "true")
    .config("spark.history.fs.update.interval", "10s")
    .config("temporaryGcsBucket", TEMP_GCS_BUCKET)
    .getOrCreate()
)

output_path = "gs://streamsonic_bucket/output/dim_sessions_batch"

checkpoint_dir = "gs://streamsonic_bucket/checkpoints/dim_sessions/"

schema = StructType([
    StructField("sessionId", IntegerType(), True),
    StructField("ts", TimestampType(), True),
])

auth_events_df = spark.read \
    .option("mergeSchema", "true") \
    .schema(schema) \
    .parquet("gs://streamsonic_bucket/listen_events/")

dimsessions_df = auth_events_df.groupBy("sessionId").agg(
    min(col("ts")).cast("timestamp").alias("startTime"),
    max(col("ts")).cast("timestamp").alias("endTime")
).select(
    col("sessionId"),
    col("startTime").cast("string"),
    col("endTime").cast("string")
).drop_duplicates(['sessionId'])

dimsessions_df.write \
    .format("bigquery") \
    .option("checkpointLocation", checkpoint_dir) \
    .option("table", "streamsonic-441414:streamsonic_dataset.dim_sessions") \
    .mode("append") \
    .save()

spark.stop()
