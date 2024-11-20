import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_trunc, when, year, month, dayofmonth, hour, minute, second, to_date, from_unixtime, unix_timestamp, count_distinct, sum
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from data_schemas import schema

def table_exists(path: str) -> bool:
        return os.path.exists(path) and spark.read.parquet(path).head(2) is not None
    
spark = SparkSession.builder \
    .appName("Fact Streams") \
    .master("local[*]") \
    .getOrCreate()

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema['listen_events']).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_listen_events")

dim_users_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/user_dimension")
dim_artists_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/dim_artist") 
dim_songs_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/song_dimension") 
dim_location_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/location_dimension")  
dim_datetime_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/datetime_dimensione")  
dim_sessions_df = spark.read.parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/session_dimension")

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/fact_streams"

fact_streams_df = listen_events_df \
    .join(
        dim_users_df,
        (
            (listen_events_df.userId == dim_users_df.userId) &  
            (to_date(listen_events_df.ts.cast("timestamp")) >= to_date(dim_users_df.rowActivationDate)) &  
            (to_date(listen_events_df.ts.cast("timestamp")) < to_date(dim_users_df.rowExpirationDate))  
        ),
        "left"
    ) \
    .join(dim_artists_df, listen_events_df.artist == dim_artists_df.artist, "left") \
    .join(dim_songs_df, (listen_events_df.artist == dim_songs_df.artist) & (listen_events_df.song == dim_songs_df.song), "left") \
    .join(dim_location_df, 
           (listen_events_df.city == dim_location_df.city) & 
           (listen_events_df.state == dim_location_df.state) & 
           (listen_events_df.lat == dim_location_df.lat) & 
           (listen_events_df.lon == dim_location_df.lon), "left") \
    .join(dim_datetime_df,
      from_unixtime(listen_events_df.ts / 1000).cast("timestamp") == dim_datetime_df.timestamp, 
      "left") \
    .join(dim_sessions_df, listen_events_df.sessionId == dim_sessions_df.sessionId, "left")

fact_streams_df = fact_streams_df.select(
    dim_users_df.userId.alias("userId"), 
    dim_artists_df.artistId.alias("artistId"),  
    dim_songs_df.songId.alias("songId"), 
    dim_location_df.locationId.alias("locationId"),  
    dim_datetime_df.datetimeId.alias("datetimeId"), 
    dim_sessions_df.sessionId.alias("sessionId"),
    dim_sessions_df.startTime.alias("startTime"),
    dim_sessions_df.endTime.alias("endTime"),
    listen_events_df.ts.alias("ts"),
    listen_events_df.duration.alias("duration") 
)

fact_streams_df = fact_streams_df.withColumn("streamCount", lit(1)) \
.withColumn(
    "is_song_skipped", 
    when(col("duration") < lit(30), 1).otherwise(0)
).withColumn(
    "listeningTime",
    (unix_timestamp(col("endTime")) - unix_timestamp(col("startTime"))) 
).groupBy("userId").agg(
    count_distinct("sessionId").alias("distinctSessions")
)

fact_streams_df = fact_streams_df.groupBy("userId").agg(
    count_distinct("sessionId").alias("distinctSessions"),
    count_distinct("songId").alias("distinctSongsPlayed"),
    sum("listeningTime").alias("totalListeningTime")  
)

fact_streams_df = fact_streams_df.select(
    "userId", 
    "artistId",  
    "songId", 
    "locationId",  
    "datetimeId", 
    "sessionId",
    "ts",  
    "streamCount", 
    "is_song_skipped", 
    "listeningTime", 
    "distinctSessions",
    "distinctSongsPlayed",
    "totalListeningTime"
)

if table_exists(output_path):
    existing_fact_df = spark.read.parquet(output_path)

    new_sessions_df = fact_streams_df.join(existing_fact_df, on=["sessionId"], how="left_anti")

    fact_streams_df.checkpoint()
    new_sessions_df.write.mode("append").parquet(output_path)
else:
    fact_streams_df.checkpoint()
    fact_streams_df.write.mode("append").parquet(output_path)

fact_streams_df.printSchema()