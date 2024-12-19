import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, hash, lit, lead, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
from pyspark.sql.window import Window
from datetime import datetime

def table_exists(path: str) -> bool:
    try:
        spark.read.format("bigquery").option("table", path).load()
        return True
    except Exception:
        return False

APP_NAME = os.getenv("APP_NAME", "Dim User Batch Processing")
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

output_table = "streamsonic-441414:streamsonic_dataset.dim_users"

checkpoint_dir = "gs://streamsonic_bucket/checkpoints/dim_users/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

schema = StructType([
    StructField("userId", LongType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("level", StringType(), True),
    StructField("ts", TimestampType(), True), 
    StructField("registration", LongType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userAgent", StringType(), True)
])

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema).parquet("gs://streamsonic_bucket/listen_events/")

user_base_df = listen_events_df.select(
    col("userId").cast("long"),
    col("firstName"),
    col("lastName"),
    col("gender"),
    col("level"),
    col("ts").alias("eventTimestamp"),
    col("registration").cast("long"),
    col("city"),
    col("zip"),
    col("state"),
    col("lon"),
    col("lat"),
    col("userAgent")
)

user_dim_df = user_base_df.withColumn(
    "rowActivationDate", col("eventTimestamp")
).withColumn(
    "rowExpirationDate", lit(datetime(9999, 12, 31))
).withColumn(
    "currRow", lit(1)
)

window_spec = Window.partitionBy("userId").orderBy("rowActivationDate")

user_dim_df = user_dim_df.withColumn(
    "rowExpirationDate",
    when(
        lead("rowActivationDate", 1).over(window_spec).isNull(),
        lit(datetime(9999, 12, 31))
    ).otherwise(lead("rowActivationDate", 1).over(window_spec))
)

user_dim_df = user_dim_df.withColumn(
    "currRow",
    when(col("rowExpirationDate") == lit(datetime(9999, 12, 31)), lit(1)) 
    .otherwise(lit(0))
)

user_dim_df = user_dim_df.dropDuplicates(["userId", "rowActivationDate"]).filter(col("currRow") == 1)

if table_exists(output_table):
    existing_user_dim_df = spark.read.format("bigquery").option("table", output_table).load()
    
    new_records_df = user_dim_df.join(existing_user_dim_df, on=["userId"], how="left_anti")
    
    new_records_df.checkpoint()

    new_records_df.write.format("bigquery") \
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
        .option("table", output_table) \
        .mode("append") \
        .save()

else:
    user_dim_df.checkpoint()

    user_dim_df.write.format("bigquery") \
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
        .option("table", output_table) \
        .mode("append") \
        .save()

spark.stop()
