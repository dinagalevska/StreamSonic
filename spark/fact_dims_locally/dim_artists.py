import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, concat_ws, hash
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from data_schemas import schema

spark = SparkSession.builder \
    .appName("Dim Artist") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/dim_artists/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

artist_dim_schema = StructType(
    [
        StructField("artistId", LongType(), True),  
        StructField("artist", StringType(), True),  
        StructField("lat", DoubleType(), True), 
        StructField("lon", DoubleType(), True), 
        StructField("city", StringType(), True),  
        StructField("city", StringType(), True),  
    ]
)

raw_listen_events_df = spark.read.option("mergeSchema", "true").schema(schema["listen_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_listen_events")

artist_data_df = raw_listen_events_df.select("artist", "lat", "lon", "city", "state")

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

final_artist_dim_df = final_artist_dim_df.select(
    "artistId", "artist", "lat", "lon", "city", "state"
).drop_duplicates(['artistId'])

final_artist_dim_df.checkpoint()

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/dim_artist"
final_artist_dim_df.write.mode("append").parquet(output_path)

