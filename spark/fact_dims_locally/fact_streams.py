import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, from_unixtime, unix_timestamp, broadcast, lag, lead, concat_ws, dayofmonth, month, year, hour, minute, second, hash, concat_ws
from pyspark.sql.window import Window
from data_schemas import schema
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("Fact Streams") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/fact_streams/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema['page_view_events']).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_page_view_events")

dim_users_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/user_dimension")
dim_artists_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/dim_artist") 
dim_songs_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/song_dimension") 
dim_location_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/location_dimension")  
dim_datetime_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/datetime_dimension")  
dim_sessions_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/session_dimension")

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/fact_streams"

listen_events_df = listen_events_df.withColumn(
    "timestamp", from_unixtime(listen_events_df.ts / 1000).cast("timestamp")
)

listen_events_df = listen_events_df.withColumn("year", year(col("timestamp"))) \
.withColumn("month", month(col("timestamp"))) \
.withColumn("day", dayofmonth(col("timestamp"))) \
.withColumn("hour", hour(col("timestamp"))) \
.withColumn("minute", minute(col("timestamp"))) \
.withColumn("second", second(col("timestamp"))) \

fact_streams_df = (
    listen_events_df.filter(col("duration").isNotNull())
    .withColumn("ArtistIdSK", col("artist").cast("long")) 
    .withColumn("timestamp", from_unixtime(col("ts") / 1000).cast("timestamp"))
    .withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("minute", minute(col("timestamp")))
    .withColumn("second", second(col("timestamp")))
    .withColumn(
        "DateTimeSK",
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
        ).cast("long"),
    )
    .withColumn(
        "LocationSK",
        hash(
            concat_ws(
                "_",
                col("city").cast("string"),
                col("zip").cast("string"),
                col("state").cast("string"),
                col("lon").cast("string"),
                col("lat").cast("string"),
            )
        ).cast("long"),
    )
    .withColumn(
        "SongSK", 
        hash(concat_ws("_", col("song").cast("string"), col("artist").cast("string"))).cast("long")
    )
    .withColumn("UserIdSK", col("userId").cast("long")) \
    .withColumn("SessionIdSK", col("sessionId").cast("long"))
)

user_window = Window.partitionBy("userId").orderBy("timestamp")

fact_streams_df = fact_streams_df.withColumn(
    "streamCount", lit(1)
).withColumn(
    "isSongSkipped", 
    when(col("duration") < lit(30), 1).otherwise(0)
).withColumn(
    "isMoving",
    when(
        (lag(col("lat")).over(user_window) == col("lat")) | 
        (lag(col("lon")).over(user_window) == col("lon")),
        0  
    ).otherwise(1) 
) \
.withColumn(
    "isFirstListenEvent",
    when(lag(col("timestamp")).over(user_window).isNull(), 1).otherwise(0)
) \
.withColumn(
    "isLastListenEvent",
    when(lead(col("timestamp")).over(user_window).isNull(), 1).otherwise(0)
) \
.withColumn(
    "nextListenEventTimeGap",
    unix_timestamp(lead(col("timestamp")).over(user_window)) - unix_timestamp(col("timestamp"))
).withColumn("consecutiveNoSong", lit(0))

fact_streams_df = fact_streams_df.withColumn(
    "rowActivationDate",
    col("ts")
).withColumn(
    "rowExpirationDate",
    lit(None).cast("long")
).withColumn(
    "currRow",
    lit(1)
).withColumn(
    "consecutiveNoSong",
    when(
        lag(col("artist")).over(user_window) == col("artist"),
        col("consecutiveNoSong") + 1,
    ).otherwise(0),
)


fact_streams_df = fact_streams_df.select(
    "UserIdSK", 
    "ArtistIdSK",  
    "SongSK", 
    "LocationSK",  
    "DateTimeSK", 
    "SessionIdSK",
    "ts",  
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "isMoving",
    "isSongSkipped", 
    "isFirstListenEvent",
    "isLastListenEvent",
    "nextListenEventTimeGap",
    "consecutiveNoSong",
    "rowActivationDate",
    "rowExpirationDate",
    "currRow"
)

fact_streams_df = fact_streams_df.filter(col("currRow") == 1)

if table_exists(output_path):
    existing_fact_df = spark.read.parquet(output_path)

    new_records_df = fact_streams_df.join(existing_fact_df, on=["userIdSK", "artistIdSK", "songIdSK", "locationIdSK", "datetimeIdSK", "sessionIdSK"], how="left_anti")

    new_records_df.checkpoint()

    new_records_df.write.mode("append").parquet(output_path)

else:
    fact_streams_df.checkpoint()
    fact_streams_df.write.mode("append").parquet(output_path)

fact_streams_df.printSchema()
