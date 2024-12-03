import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, lag, lit, when, rank, sum as _sum, first, from_unixtime
from pyspark.sql.window import Window
from datetime import datetime
import pyspark.sql.functions as F
from data_schemas import schema

def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("SCD Type 2 for User Dimension") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/user_dim/"

spark.sparkContext.setCheckpointDir(checkpoint_dir)

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/user_dimension"

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema['listen_events']).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_listen_events")

user_base_df = listen_events_df.select(
    col("userId").cast("long"),
    col("firstName"),
    col("lastName"),
    col("gender"),
    col("level"),
    col("ts").alias("eventTimestamp"),
    col("registration").cast("long"),
    col("city"),
    col("zip"),
    col("state"),
    col("lon"),
    col("lat"),
    col('userAgent')
)

final_user_dim_df = user_base_df.withColumn(
    "rowActivationDate", col("eventTimestamp")
).withColumn(
    "rowExpirationDate", lit(datetime(9999, 12, 31))
).withColumn(
    "currRow", lit(1)
)

window_spec = Window.partitionBy("userId").orderBy("rowActivationDate")

final_user_dim_df = final_user_dim_df.withColumn(
    "rowExpirationDate",
    when(
        lead("rowActivationDate", 1).over(window_spec).isNull(),
        lit(datetime(9999, 12, 31))
    ).otherwise(lead("rowActivationDate", 1).over(window_spec))
)

final_user_dim_df = final_user_dim_df.withColumn(
    "currRow",
    when(col("rowExpirationDate") ==  lit(datetime(9999, 12, 31)), lit(1)) 
    .otherwise(lit(0))
)

final_user_dim_df = final_user_dim_df.dropDuplicates(["userId", "rowActivationDate"]).filter(col("currRow") == 1)

if table_exists(output_path):
    existing_user_dim_df = spark.read.parquet(output_path)

    new_records_df = final_user_dim_df.join(existing_user_dim_df, on=["userId"], how="left_anti")

    new_records_df.checkpoint()

    new_records_df.write.mode("append").parquet(output_path)

else:
    final_user_dim_df.checkpoint()
    final_user_dim_df.write.mode("append").parquet(output_path)

final_user_dim_df.printSchema()
final_user_dim_df.show(5)
