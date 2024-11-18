import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, sha2, concat_ws, hash
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from data_schemas import schema

spark = SparkSession.builder \
    .appName("Dim Location") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/location_dim/"

spark.sparkContext.setCheckpointDir(checkpoint_dir)

location_dim_schema = StructType(
    [
        StructField("locationId", LongType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("state", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
    ]
)

raw_page_view_events_df = spark.read.option("mergeSchema", "true").schema(schema["page_view_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_page_view_events")

combined_df = raw_page_view_events_df.select("city","zip", "state", "lat", "lon")


final_location_dim_df = combined_df.withColumn(
    "locationId",
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
final_location_dim_df = final_location_dim_df.select(
    "locationId", "city","zip", "state", "lat", "lon"
).drop_duplicates(['locationId'])

final_location_dim_df.checkpoint()

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/location_dimension"
final_location_dim_df.write.mode("append").parquet(output_path)

# final_location_dim_df.show()
