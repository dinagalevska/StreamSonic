import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, concat_ws, hash, lead, when, first, sum as _sum, lag, from_unixtime
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from data_schemas import schema

def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("Dim Artist") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/dim_artists/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/dim_artist"

raw_listen_events_df = spark.read.option("mergeSchema", "true").schema(schema["listen_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_listen_events")

artist_data_df = raw_listen_events_df.select("artist", "lat", "lon", "city", "state", "ts")                      

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

final_artist_dim_df = final_artist_dim_df.select('artistId', "artist", "lat", "lon", "city", "state", "ts") \
    .drop_duplicates(['artistId'])

final_artist_dim_df = final_artist_dim_df.withColumn(
    "rowActivationDate", col("ts")
).withColumn(
    "rowExpirationDate", lit(None).cast("long")  
).withColumn(
    "currRow", lit(1)
)

window_spec = Window.partitionBy("artistId").orderBy("rowActivationDate")

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

final_artist_dim_df = final_artist_dim_df.dropDuplicates(["artistId", "rowActivationDate"]).filter(col("currRow") == 1)

if table_exists(output_path):
    existing_artist_dim_df = spark.read.parquet(output_path)

    new_records_df = final_artist_dim_df.join(existing_artist_dim_df, on=["artistId"], how="left_anti")
    new_records_df.checkpoint()

    new_records_df.write.mode("append").parquet(output_path)
else:
    final_artist_dim_df.checkpoint()
    final_artist_dim_df.write.mode("append").parquet(output_path)

final_artist_dim_df.printSchema()