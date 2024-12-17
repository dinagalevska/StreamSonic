import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hash, concat_ws, lead, when, lit
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Function to check if the path exists (optional)
def table_exists(path: str) -> bool:
    return os.path.exists(path)

# Create Spark session
spark = SparkSession.builder \
    .appName("Dim Artist Streaming") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.gs.auth.service.account.enable", "true") \
    .config("spark.history.fs.update.interval", "10s") \
    .config("temporaryGcsBucket", "streamsonic_bucket") \
    .getOrCreate()

# Checkpoint directory should be a persistent GCS path
checkpoint_dir = "gs://streamsonic_bucket/checkpoints/dim_artists/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

# Output path (could be used for local debug, or removed for BigQuery write)
output_path = "gs://streamsonic_bucket/output/dim_artist"

# Read schema for incoming data (adjust schema based on your actual data)
schema = StructType([
    StructField("artist", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("ts", LongType(), True),
])

# Read stream from the GCS bucket (replace with your actual Datalake path)
raw_listen_events_df = spark.readStream \
    .option("mergeSchema", "true") \
    .schema(schema) \
    .parquet("gs://streamsonic_bucket/listen_events/")

# Transformation logic to create the artist dimension
artist_data_df = raw_listen_events_df.select("artist", "lat", "lon", "city", "state", "ts")

# Creating artistId (hash of artist + geo + city/state)
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

# Remove duplicates based on artistId
final_artist_dim_df = final_artist_dim_df.select('artistId', "artist", "lat", "lon", "city", "state", "ts") \
    .drop_duplicates(['artistId'])

# Set row activation and expiration logic
final_artist_dim_df = final_artist_dim_df.withColumn(
    "rowActivationDate", col("ts")
).withColumn(
    "rowExpirationDate", lit(None).cast("long")
).withColumn(
    "currRow", lit(1)
)

# Create window specification for row expiration date logic
window_spec = Window.partitionBy("artistId").orderBy("rowActivationDate")

# Handle row expiration and current row flags
final_artist_dim_df = final_artist_dim_df \
    .withColumn(
        "rowExpirationDate",
        when(
            lead("rowActivationDate", 1).over(window_spec).isNull(),
            lit(datetime(9999, 12, 31))
        ).otherwise(lead("rowActivationDate", 1).over(window_spec))
    ) \
    .withColumn(
        "currRow",
        when(col("rowExpirationDate") == lit(datetime(9999, 12, 31)), lit(1))
        .otherwise(lit(0))
    )

# Filter for the latest active rows
final_artist_dim_df = final_artist_dim_df.dropDuplicates(["artistId", "rowActivationDate"]).filter(col("currRow") == 1)

# Write the stream to BigQuery
final_artist_dim_df.writeStream \
    .format("bigquery") \
    .option("checkpointLocation", checkpoint_dir) \
    .option("table", "streamsonic-441414:streamsonic_dataset.dim_artist") \
    .outputMode("append") \
    .start()

# Await termination to keep the stream running
spark.streams.awaitAnyTermination()
