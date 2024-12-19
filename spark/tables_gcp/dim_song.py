import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, hash, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from datetime import datetime

def table_exists(path: str) -> bool:
    try:
        spark.read.format("bigquery").option("table", path).load()
        return True
    except Exception:
        return False

APP_NAME = os.getenv("APP_NAME", "Dim Songs Batch Processing")
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

output_table = "streamsonic-441414:streamsonic_dataset.dim_songs"

checkpoint_dir = "gs://streamsonic_bucket/checkpoints/dim_songs/"

spark.sparkContext.setCheckpointDir(checkpoint_dir)

schema = StructType([
    StructField("song", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("ts", TimestampType(), True) 
])

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema).parquet("gs://streamsonic_bucket/listen_events/")

song_data_df = listen_events_df.select("song", "artist", "duration", "ts")
song_dim_df = song_data_df.withColumn(
    "songId",
    hash(concat_ws("_", col("song"), col("artist"))).cast("long")
)

song_dim_df = song_dim_df.select("songId", "artist", "song", "duration", "ts").drop_duplicates(['songId'])

if table_exists(output_table):
    existing_song_dim_df = spark.read.format("bigquery").option("table", output_table).load()
    
    new_records_df = song_dim_df.join(existing_song_dim_df, on=["songId"], how="left_anti")
    
    new_records_df.checkpoint()

    new_records_df.write.format("bigquery") \
        .option("temporaryGcsBucket", "streamsonic_bucket") \
        .option("table", output_table) \
        .mode("append") \
        .save()

else:
    song_dim_df.checkpoint()

    song_dim_df.write.format("bigquery") \
        .option("temporaryGcsBucket", "streamsonic_bucket") \
        .option("table", output_table) \
        .mode("append") \
        .save()

spark.stop()
