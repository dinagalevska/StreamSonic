import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, unix_timestamp, broadcast, lag, lead, concat_ws, dayofmonth, month, year, hour, minute, second, hash, concat_ws, coalesce
from pyspark.sql.window import Window
from datetime import datetime
from data_schemas import schema
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from pyspark.sql import functions as F

def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("Fact Streams") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/fact_streams_2_datetime/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema["listen_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_listen_events")

dim_users_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/user_dimension")
dim_artists_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/dim_artist") 
dim_songs_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/song_dimension") 
dim_location_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/location_dimension")  
dim_datetime_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/datetime_dimension_with_time")  
dim_sessions_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/session_dimension")

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/fact_streams_2_datetime"

user_window = Window.partitionBy("userId").orderBy("ts")

listen_events_df = listen_events_df.withColumn(
    "LocationKey", 
    hash(
        concat_ws(
            "_",
            coalesce(col("city"), lit("Unknown")),
            coalesce(col("zip"), lit("Unknown")),
            coalesce(col("state"), lit("Unknown")),
            coalesce(col("lon"), lit("0")),
            coalesce(col("lat"), lit("0")),
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
.withColumn(
    "streamCount", F.lit(1)
).withColumn(
    "isSongSkipped", 
    F.when(F.col("duration").cast("double")  < F.lit(30), 1).otherwise(0)
).withColumn(
    "isMoving",
    F.when(
        (F.lag(F.col("lat")).over(user_window) == col("lat"))
        | (F.lag(F.col("lon")).over(user_window) == col("lon")),
        0,
        ).otherwise(1),
).withColumn(
    "isFirstListenEvent",
    F.when(F.lag(F.col("ts")).over(user_window).isNull(), 1).otherwise(0)
).withColumn(
    "isLastListenEvent",
    F.when(F.lead(F.col("ts")).over(user_window).isNull(), 1).otherwise(0)
).withColumn(
    "nextListenEventTimeGap",
    (unix_timestamp(F.lead("ts").over(user_window)) - unix_timestamp("ts")).cast("long")
).withColumn("consecutiveNoSong", F.lit(0))

fact_streams_df = (
    listen_events_df.alias("listen_events")
    .join(
        broadcast(dim_users_df.filter(col("currRow") == 1).alias("dim_users")),
        (col("listen_events.userId") == col("dim_users.userId")),
        how="left"
    )
    .join(
        dim_artists_df.filter(col("currRow") == 1).alias("dim_artists"),
        (col("listen_events.ArtistKey") == col("dim_artists.artistId")),
        how="left"
    )
    .join(
        dim_songs_df.filter(col("currRow") == 1).alias("dim_songs"),
        (col("listen_events.SongKey") == col("dim_songs.songId")),
        how="left"
    )
    .join(
        dim_location_df.filter(col("currRow") == 1).alias("dim_location"),
        (col("listen_events.LocationKey") == col("dim_location.locationId")),
        how="left"
    )
    .join(
        dim_datetime_df.alias("dim_datetime"),  # No filter for currRow
        (F.date_trunc('hour', col("listen_events.ts")) == col("dim_datetime.datetime")),
        how="left"
    )
    .join(
        dim_sessions_df.filter(col("currRow") == 1).alias("dim_sessions"),
        (col("listen_events.sessionId") == col("dim_sessions.sessionId")),
        how="left"
    )
    .select(
        col("dim_users.userId").alias("UserKey"),
        col("dim_artists.artistId").alias("ArtistKey"),
        col("dim_songs.songId").alias("SongKey"),
        col('listen_events.duration').cast('double'),
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
        col("listen_events.year"),
        col("listen_events.month"),
        col("listen_events.day"),
        col("listen_events.hour"),
        col("listen_events.minute"),
        col("listen_events.second"),
    )
)


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

# fact_streams_df = fact_streams_df.orderBy('Timestamp', ascending=False)

fact_streams_df = fact_streams_df.dropDuplicates(['UserKey', 'ArtistKey', 'SongKey', 'SessionKey', 'DateKey', 'Timestamp'])

fact_streams_df.checkpoint()
if table_exists(output_path):
    existing_fact_df = spark.read.parquet(output_path)
    existing_fact_df.checkpoint()

    new_records_df = fact_streams_df.join(
        existing_fact_df,
        on=["UserKey", "ArtistKey", "SongKey", "LocationKey", "DateKey", "rowActivationDate"],
        how="left"
    )
    new_records_df.write.mode("append").parquet(output_path)

else:
    fact_streams_df.write.mode("append").parquet(output_path)

fact_streams_df.printSchema()
