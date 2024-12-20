import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, lit, when, unix_timestamp, broadcast, lag, lead, concat_ws, dayofmonth, month, year, hour, minute, second, hash, coalesce
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType, IntegerType
from datetime import datetime
from pyspark.sql import functions as F
from google.cloud.exceptions import NotFound
from google.cloud import bigquery


def table_exists(path: str) -> bool:
    return os.path.exists(path)

BIGQUERY_PROJECT = os.getenv("PROJECT_ID", "streamsonic-441414")  
BIGQUERY_DATASET = os.getenv("BQ_DATASET", "streamsonic_dataset")  
APP_NAME = os.getenv("APP_NAME", "Fact Streans")
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

schema = StructType([
        StructField("artist", StringType(), True),
        StructField("song", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("ts", TimestampType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("auth", StringType(), True),
        StructField("level", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True), 
        StructField("state", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("userId", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration", LongType(), True)
    ])

CHECKPOINT_DIR = "gs://streamsonic_bucket/checkpoints/fact_streams/" 
spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema).parquet("gs://streamsonic_bucket/listen_events/")
dim_users_df = spark.read.format("bigquery").option("table", f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.dim_users").load()
dim_artists_df = spark.read.format("bigquery").option("table", f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.dim_artist").load()
dim_songs_df = spark.read.format("bigquery").option("table", f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.dim_songs").load()
dim_location_df = spark.read.format("bigquery").option("table", f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.dim_location").load()
dim_datetime_df = spark.read.format("bigquery").option("table", f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.dim_datetime").load()
dim_sessions_df = spark.read.format("bigquery").option("table", f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.dim_sessions").load()

user_window = Window.partitionBy("userId").orderBy("ts")

listen_events_df = listen_events_df.withColumn(
    "LocationKey", 
    hash(
        concat_ws(
            "_", 
            col("city").cast("string"),
            col("zip").cast("string"),
            col("state").cast("string"),
            col("lat").cast("string"),
            col("lon").cast("string")
        )
    ).cast("long"),
).withColumn(
    "ArtistKey", 
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
).withColumn(
    "SongKey",
    hash(
        concat_ws(
            "_",
            col("song"),
            col("artist")
        )
    ).cast("long")
).withColumn("year", year(col("ts"))) \
.withColumn("month", month(col("ts"))) \
.withColumn("day", dayofmonth(col("ts"))) \
.withColumn("hour", hour(col("ts"))) \
.withColumn("minute", minute(col("ts"))) \
.withColumn("second", second(col("ts"))) \
.withColumn("streamCount", lit(1)) \
.withColumn(
    "DateKey",
    concat_ws(
        "",
        col("year"),
        lpad(col("month"), 2, "0"),
        lpad(col("day"), 2, "0"),
        lpad(col("hour"), 2, "0")
    )
) \
.withColumn(
    "isSongSkipped", 
    when(col("duration").cast("double") < lit(30), 1).otherwise(0)
).withColumn(
    "isMoving",
    when(
        (lag(col("lat")).over(user_window) == col("lat"))
        & (lag(col("lon")).over(user_window) == col("lon")),
        0
    ).otherwise(1),
).withColumn(
    "isFirstListenEvent",
    when(lag(col("ts")).over(user_window).isNull(), 1).otherwise(0)
).withColumn(
    "isLastListenEvent",
    when(lead(col("ts")).over(user_window).isNull(), 1).otherwise(0)
).withColumn(
    "nextListenEventTimeGap",
    (unix_timestamp(lead("ts").over(user_window)) - unix_timestamp("ts")).cast("long")
).withColumn("consecutiveNoSong", lit(0))

listen_events_df = listen_events_df.checkpoint()

fact_streams_df = (
    listen_events_df.alias("listen_events")
    .join(
        broadcast(dim_users_df.filter(col("currRow") == 1).alias("dim_users")),
        col("listen_events.userId") == col("dim_users.userId"),
        how="left"
    )
    .join(
        dim_artists_df.filter(col("currRow") == 1).alias("dim_artists"),
        col("listen_events.ArtistKey") == col("dim_artists.artistId"),
        how="left"
    )
    .join(
        dim_songs_df.alias("dim_songs"),
        col("listen_events.SongKey") == col("dim_songs.songId"),
        how="left"
    )
    .join(
        dim_location_df.filter(col("currRow") == 1).alias("dim_location"),
        col("listen_events.LocationKey") == col("dim_location.locationId"),
        how="left"
    )
    .join(
        dim_datetime_df.alias("dim_datetime"),
        col("listen_events.DateKey") == col("dim_datetime.datetime"),
        how="left"
    )
    .join(
        dim_sessions_df.alias("dim_sessions"),
        col("listen_events.sessionId") == col("dim_sessions.sessionId"),
        how="left"
    )
    .select(
        col("dim_users.userId").alias("UserKey"),
        col("dim_artists.artistId").alias("ArtistKey"),
        col("dim_songs.songId").alias("SongKey"),
        col("dim_datetime.datetime").alias("DateKey"),
        col("dim_location.locationId").alias("LocationKey"),
        col("dim_sessions.sessionId").alias("SessionKey"),
        col("listen_events.ts").alias("Timestamp"),
        col("listen_events.streamCount"),
        col("listen_events.isSongSkipped"),
        col("listen_events.isMoving"),
        col("listen_events.isFirstListenEvent"),
        col("listen_events.isLastListenEvent"),
        col("listen_events.nextListenEventTimeGap"),
        col("listen_events.consecutiveNoSong"),
    )
)

fact_streams_df = fact_streams_df.dropDuplicates(['UserKey', 'ArtistKey', 'SongKey', 'SessionKey', 'DateKey', 'Timestamp'])

window_spec = Window.partitionBy("UserKey", "ArtistKey", "SongKey", "LocationKey", "DateKey").orderBy("Timestamp")

fact_streams_df = fact_streams_df.withColumn(
    "rowActivationDate", col("Timestamp")
).withColumn(
    "rowExpirationDate",
    when(
        lead("Timestamp").over(window_spec).isNull(),
        lit(datetime(9999, 12, 31)) 
    ).otherwise(lead("Timestamp").over(window_spec))
).withColumn(
    "currRow",
    when(col("rowExpirationDate") == lit(datetime(9999, 12, 31)), lit(1)).otherwise(lit(0))
)

fact_streams_df = fact_streams_df.checkpoint()

OUTPUT_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.fact_streams"

def bigquery_table_exists(project_id, dataset_id, table_name):
    client = bigquery.Client(project=project_id)
    try:
        client.get_table(f"{project_id}.{dataset_id}.{table_name}")
        return True
    except NotFound:
        return False

table_exists = bigquery_table_exists(BIGQUERY_PROJECT, BIGQUERY_DATASET, "fact_streams")

if table_exists:
    existing_fact_df = spark.read.format("bigquery").option("table", OUTPUT_TABLE).load()

    new_records_df = fact_streams_df.join(
        existing_fact_df,
        on=["UserKey", "ArtistKey", "SongKey", "LocationKey", "DateKey", "rowActivationDate"],
        how="left_anti"
    )

    if new_records_df.rdd.isEmpty():
        print("No new records to append.")
    else:
        new_records_df.write \
            .format("bigquery") \
            .option("table", OUTPUT_TABLE) \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        print(f"Appended {new_records_df.count()} new records to the BigQuery table.")
else:
    fact_streams_df.write \
        .format("bigquery") \
        .option("table", OUTPUT_TABLE) \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save()
    print("BigQuery table created and data written.")
