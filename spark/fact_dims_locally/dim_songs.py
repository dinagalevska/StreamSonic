import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, hash
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from data_schemas import schema

spark = SparkSession.builder \
    .appName("Dim Songs") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/song_dim/"

spark.sparkContext.setCheckpointDir(checkpoint_dir)

song_dim_schema = StructType(
    [
        StructField("songId", LongType(), True),  
        StructField("song", StringType(), True),  
        StructField("artist", StringType(), True),  
        StructField("duration", DoubleType(), True),  
    ]
)

raw_songs_df = spark.read.option("mergeSchema", "true").schema(schema["listen_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_listen_events")

song_data_df = raw_songs_df.select("song", "artist", "duration")

final_song_dim_df = song_data_df.withColumn(
    "songId",
    hash(
        concat_ws(
            "_",
            col("song").cast("string"),
            col("artist").cast("string"),
            col("duration").cast("double"),
        )
    ).cast("long")
)

final_song_dim_df = final_song_dim_df.select(
    "songId", "song", "artist", "duration"
).drop_duplicates(['songId'])

final_song_dim_df.checkpoint()

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/song_dimension"
final_song_dim_df.write.mode("append").parquet(output_path)

# final_song_dim_df.show()
