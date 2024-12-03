import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, sha2, concat_ws, hash, sum, lag, lead, when, first, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from data_schemas import schema
from pyspark.sql.window import Window
from datetime import datetime

def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("Dim Location") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/location_dim/"

spark.sparkContext.setCheckpointDir(checkpoint_dir)

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/location_dimension"

raw_page_view_events_df = spark.read.option("mergeSchema", "true").schema(schema["page_view_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_page_view_events")

combined_df = raw_page_view_events_df.select("city", "zip", "state", "lon", "lat", "ts")

final_location_dim_df = combined_df.withColumn(
    "locationId",
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
)

final_location_dim_df =  final_location_dim_df.select('locationId', "city", "zip", "state", "lon", "lat", "ts").drop_duplicates(['locationId'])

window_spec = Window.partitionBy("locationId").orderBy("ts")

location_changes_df = final_location_dim_df.withColumn(
    "rowActivationDate", col("ts")  
).withColumn(
    "rowExpirationDate", lit(datetime(9999, 12, 31)) 
).withColumn(
    "currRow", lit(1)  
)

window_group_spec = Window.partitionBy("locationId").orderBy("rowActivationDate")


activation_df = location_changes_df.withColumn(
    "rowExpirationDate",
    when(
        lead("rowActivationDate", 1).over(window_group_spec).isNull(),
        lit(datetime(9999, 12, 31))  
    ).otherwise(lead("rowActivationDate", 1).over(window_group_spec)) 
)

final_location_dim_df = activation_df.withColumn(
    "currRow",
    when(col("rowExpirationDate") == lit(datetime(9999, 12, 31)), lit(1)) 
    .otherwise(lit(0))
)

final_location_dim_df = final_location_dim_df.select(
    "locationId",
    "city",
    "zip",
    "state",
    "lon",
    "lat",
    "rowActivationDate",
    "rowExpirationDate",
    "currRow"
)

final_location_dim_df = final_location_dim_df.dropDuplicates(["locationId", "rowActivationDate"])

final_location_dim_df = final_location_dim_df.filter(col("currRow") == 1)


if table_exists(output_path):
    existing_location_dim_df = spark.read.parquet(output_path)

    new_records_df = final_location_dim_df.join(existing_location_dim_df, on=["locationId"], how="left_anti")

    new_records_df.checkpoint()

    new_records_df.write.mode("append").parquet(output_path)

else:
    final_location_dim_df.checkpoint()
    final_location_dim_df.write.mode("append").parquet(output_path)

final_location_dim_df.printSchema()