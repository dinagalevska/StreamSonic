import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hash, concat_ws, lead, when, lit, unix_timestamp
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

def table_exists(path: str) -> bool:
    return os.path.exists(path)

APP_NAME = os.getenv("APP_NAME", "Dim Artist Batch Processing")
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

output_path = "gs://streamsonic_bucket/output/dim_artist_batch"

checkpoint_dir = "gs://streamsonic_bucket/checkpoints/dim_artists/"

schema = StructType([
    StructField("artist", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("ts", TimestampType(), True),
])

raw_listen_events_df = spark.read \
    .schema(schema) \
    .parquet("gs://streamsonic_bucket/listen_events/")

artist_data_df = raw_listen_events_df.select("artist", "lat", "lon", "city", "state", "ts")

final_artist_dim_df = artist_data_df.withColumn(
    "artistId",
    hash(
        concat_ws(
            "_", 
            col("artist").cast("string"),
            col("lat").cast("string"),
            col("lon").cast("string"),
            col("city").cast("string"),
            col("state").cast("string")
        )
    ).cast("long")
)

final_artist_dim_df = final_artist_dim_df.select('artistId', "artist", "lat", "lon", "city", "state", "ts") \
    .drop_duplicates(['artistId'])

final_artist_dim_df = final_artist_dim_df.withColumn(
    "rowActivationDate", col("ts")
).withColumn(
    "rowExpirationDate", lit(None).cast("long")  
).withColumn(
    "currRow", lit(1)
)

window_spec = Window.partitionBy("artistId").orderBy("rowActivationDate")

max_timestamp_epoch = unix_timestamp(lit("9999-12-31 00:00:00"), "yyyy-MM-dd HH:mm:ss").cast("long")

final_artist_dim_df = final_artist_dim_df \
    .withColumn(
        "rowExpirationDate",
        when(
            lead("rowActivationDate", 1).over(window_spec).isNull(),
            max_timestamp_epoch  
        ).otherwise(unix_timestamp(lead("rowActivationDate", 1).over(window_spec)))
    ) \
    .withColumn(
        "currRow",
        when(col("rowExpirationDate") == max_timestamp_epoch, lit(1))
        .otherwise(lit(0))
    )

final_artist_dim_df = final_artist_dim_df.dropDuplicates(["artistId", "rowActivationDate"]).filter(col("currRow") == 1)

final_artist_dim_df.write \
    .format("bigquery") \
    .option("checkpointLocation", checkpoint_dir) \
    .option("table", "streamsonic-441414:streamsonic_dataset.dim_artist") \
    .mode("append") \
    .save()

spark.stop()
