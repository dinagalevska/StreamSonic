import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, hash, lag, when, lead, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from data_schemas import schema
from pyspark.sql.window import Window
from datetime import datetime

def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("Dim Songs") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/song_dim/"

spark.sparkContext.setCheckpointDir(checkpoint_dir)

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/song_dimension"

raw_songs_df = spark.read.option("mergeSchema", "true").schema(schema["listen_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_listen_events")

song_data_df = raw_songs_df.select("song", "artist", "duration")

song_dim_df = song_data_df.withColumn(
    "songId",
    hash(
        concat_ws(
            "_",
            col("song"),
            col("artist")
        )
    ).cast("long")
)

song_dim_df = song_dim_df.select("songId", "artist", "song", "duration").drop_duplicates(['songId'])

window_spec = Window.partitionBy("songId").orderBy("song", "artist", "duration")

song_changes_df = song_dim_df.withColumn(
    "prevArtist", lag("artist", 1).over(window_spec)
).withColumn(
    "prevSong", lag("song", 1).over(window_spec)
).withColumn(
    "prevDuration", lag("duration", 1).over(window_spec)
).withColumn(
    "isSongChanged",
    when(
        (col("prevArtist") != col("artist")) |
        (col("prevSong") != col("song")) |
        (col("prevDuration") != col("duration")),
        lit(1)
    ).otherwise(lit(0))
)

activation_df = song_changes_df.withColumn(
    "rowActivationDate",
    when(col("isSongChanged") == 1, current_timestamp()).otherwise(lit(None))
)

window_group_spec = Window.partitionBy("songId").orderBy("rowActivationDate")

final_song_df = activation_df.withColumn(
    "rowExpirationDate",
    lead("rowActivationDate", 1).over(window_group_spec)
).withColumn(
    "rowExpirationDate",
    when(col("rowExpirationDate").isNull(), lit("9999-12-31")).otherwise(col("rowExpirationDate"))
).withColumn(
    "rowExpirationDate",
    col("rowExpirationDate").cast("timestamp")
).withColumn(
    "currRow",
    when(col("rowExpirationDate") == lit("9999-12-31"), lit(1)).otherwise(lit(0))
)

final_song_df = final_song_df.select(
    "songId",
    "song",
    "artist",
    "duration",
    "rowActivationDate",
    "rowExpirationDate",
    "currRow"
)

final_song_df = final_song_df.dropDuplicates(["songId", "rowActivationDate"])

final_song_df = final_song_df.filter(col("currRow") == 1)

if table_exists(output_path):
    existing_song_dim_df = spark.read.parquet(output_path)

    new_records_df = final_song_df.join(existing_song_dim_df, on=["songId"], how="left_anti")

    new_records_df.checkpoint()

    new_records_df.write.mode("append").parquet(output_path)

else:
    final_song_df.checkpoint()
    final_song_df.write.mode("append").parquet(output_path)

final_song_df.printSchema()