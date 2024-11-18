from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_trunc, when, year, month, dayofmonth, hour, minute, second
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from data_schemas import schema

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

fact_streams_df = listen_events_df \
    .join(dim_users_df, listen_events_df.userId == dim_users_df.userId, "left") \
    .join(dim_artists_df, listen_events_df.artist == dim_artists_df.name, "left") \
    .join(dim_songs_df, (listen_events_df.artist == dim_songs_df.artist) & (listen_events_df.song == dim_songs_df.song), "left") \
    .join(dim_location_df, 
           (listen_events_df.city == dim_location_df.city) & 
           (listen_events_df.state == dim_location_df.state) & 
           (listen_events_df.lat == dim_location_df.lat) & 
           (listen_events_df.lon == dim_location_df.lon), "left") \
    .join(dim_datetime_df, 
          (year(listen_events_df.ts.cast("timestamp")) == dim_datetime_df.year) & 
          (month(listen_events_df.ts.cast("timestamp")) == dim_datetime_df.month) & 
          (dayofmonth(listen_events_df.ts.cast("timestamp")) == dim_datetime_df.day) & 
          (hour(listen_events_df.ts.cast("timestamp")) == dim_datetime_df.hour) & 
          (minute(listen_events_df.ts.cast("timestamp")) == dim_datetime_df.minute) & 
          (second(listen_events_df.ts.cast("timestamp")) == dim_datetime_df.second), 
          "left")

fact_streams_df = fact_streams_df.select(
    dim_users_df.userId.alias("userId"), 
    dim_artists_df.artistId.alias("artistId"),  
    dim_songs_df.songId.alias("songId"), 
    dim_location_df.locationId.alias("locationId"),  
    dim_datetime_df.datetimeId.alias("datetimeId"), 
    listen_events_df.ts.alias("ts")  
)

fact_streams_df = fact_streams_df.withColumn("streamCount", lit(1))  

fact_streams_df.checkpoint()

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/fact_streams"

fact_streams_df.write.mode("append").parquet(output_path)

# fact_streams_df.printSchema()
# fact_streams_df.show(5)
