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

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/fact_streams/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema["listen_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_listen_events")

dim_users_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/user_dimension")
dim_artists_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/dim_artist") 
dim_songs_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/song_dimension") 
dim_location_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/location_dimension")  
dim_datetime_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/datetime_dimension")  
dim_sessions_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/session_dimension")

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/fact_streams"

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
        "DateKey",
        hash(
            concat_ws(
                "_",
                col("year").cast("string"),
                col("month").cast("string"),
                col("day").cast("string"),
                col("hour").cast("string"),
                col("minute").cast("string"),
                col("second").cast("string"),
            )
        ).cast("long")
).withColumn(
    "streamCount", F.lit(1)
).withColumn(
    "isSongSkipped", 
    F.when(F.col("duration").cast("double")  < F.lit(30), 1).otherwise(0)
).withColumn(
    "isMoving",
    F.when(
        (F.lag(F.col("lat")).over(user_window).isNull()) | 
        (F.lag(F.col("lon")).over(user_window).isNull()) | 
        (F.lag(F.col("lat")).over(user_window) != F.col("lat")) |
        (F.lag(F.col("lon")).over(user_window) != F.col("lon")),
        1
    ).otherwise(0)
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

print("da")
fact_streams_df = (
    listen_events_df.alias("listen_events")
    .join(
        broadcast(dim_users_df.alias("dim_users")),
        (col("listen_events.userId") == col("dim_users.userId")) & 
        (col("listen_events.ts").between(col("dim_users.rowActivationDate"), col("dim_users.RowExpirationDate"))),
        how="left"  
    )
    .join(
        dim_artists_df.alias("dim_artists"),
        (col("listen_events.ArtistKey") == col("dim_artists.artistId")) & 
        (col("listen_events.ts").between(col("dim_artists.rowActivationDate"), col("dim_artists.RowExpirationDate"))),
        how="left"
    )
    .join(
        dim_songs_df.alias("dim_songs"),
        (col("listen_events.SongKey") == col("dim_songs.songId")) & 
        (col("listen_events.ts").between(col("dim_songs.rowActivationDate"), col("dim_songs.RowExpirationDate"))),
        how="left"
    )
    .join(
        dim_location_df.alias("dim_location"),
        (col("listen_events.LocationKey") == col("dim_location.locationId")) & 
        (col("listen_events.ts").between(col("dim_location.rowActivationDate"), col("dim_location.RowExpirationDate"))),
        how="left"
    )
    .join(
        dim_datetime_df.alias("dim_datetime"),
        (col("listen_events.DateKey") == col("dim_datetime.datetimeId")) & 
        (col("listen_events.ts").between(col("dim_datetime.rowActivationDate"), col("dim_datetime.RowExpirationDate"))),
        how="left"
    )
    .join(
        dim_sessions_df.alias("dim_sessions"),
        (col("listen_events.sessionId") == col("dim_sessions.sessionId")) & 
        (col("listen_events.ts").between(col("dim_sessions.rowActivationDate"), col("dim_sessions.RowExpirationDate"))),
        how="left"
    )
    .select(
        col("dim_users.userId").alias("UserKey"),
        col("dim_artists.artistId").alias("ArtistKey"),
        col("dim_songs.songId").alias("SongKey"),
        col("dim_datetime.datetimeId").alias("DateKey"),
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
        col("listen_events.second")
    )
)
print("da")

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

fact_streams_df = fact_streams_df.dropDuplicates(["UserKey", "ArtistKey", "SongKey", "LocationKey", "DateKey", "rowActivationDate"])

fact_streams_df.checkpoint()
if table_exists(output_path):
    existing_fact_df = spark.read.parquet(output_path)
    existing_fact_df.checkpoint()

    new_records_df = fact_streams_df.join(
        existing_fact_df,
        on=["UserKey", "ArtistKey", "SongKey", "LocationKey", "DateKey", "rowActivationDate"],
        how="left_anti"
    )
    new_records_df.write.mode("append").parquet(output_path)

else:
    fact_streams_df.write.mode("append").parquet(output_path)

fact_streams_df.printSchema()
