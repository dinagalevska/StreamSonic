import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, hash, from_unixtime, current_timestamp, lit, min, max, lag, lead, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from data_schemas import schema
from pyspark.sql.window import Window
from datetime import datetime

def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("Session Dimension") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/sessions_dim/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/session_dimension"

# auth_events_df = spark.read.option("mergeSchema", "true").schema(schema['auth_events']).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_auth_events")

auth_events_df =  spark.read.option("mergeSchema", "true").schema(schema['listen_events']).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_listen_events")

dimsessions_df = auth_events_df.groupBy("sessionId").agg(
    min(col("ts")).cast("timestamp").alias("startTime"),
    max(col("ts")).cast("timestamp").alias("endTime")
).select(
    col("sessionId"),
    col("startTime").cast("string"),
    col("endTime").cast("string")
).drop_duplicates(['sessionId'])

window_spec = Window.partitionBy("sessionId").orderBy("startTime", "endTime")

session_changes_df = dimsessions_df.withColumn(
    "prevStartTime", lag("startTime", 1).over(window_spec)
).withColumn(
    "prevEndTime", lag("endTime", 1).over(window_spec)
).withColumn(
    "isSessionChanged",
    when(
        (col("prevStartTime") != col("startTime")) |
        (col("prevEndTime") != col("endTime")),
        lit(1)
    ).otherwise(lit(0))
)

activation_df = session_changes_df.withColumn(
    "rowActivationDate",
    when(col("isSessionChanged") == 1, col("startTime")).otherwise(col("startTime"))
)

window_group_spec = Window.partitionBy("sessionId").orderBy("rowActivationDate")

final_sessions_df = activation_df.withColumn(
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

final_sessions_df = final_sessions_df.select(
    "sessionId",
    "startTime",
    "endTime",
    "rowActivationDate",
    "rowExpirationDate",
    "currRow"
)

final_sessions_df = final_sessions_df.dropDuplicates(["sessionId", "rowActivationDate"])

final_sessions_df = final_sessions_df.filter(col("currRow") == 1)

if table_exists(output_path):
    existing_sessions_df = spark.read.parquet(output_path)

    new_sessions_df = final_sessions_df.join(existing_sessions_df, on=["sessionId"], how="left_anti")

    new_sessions_df.checkpoint()
    new_sessions_df.write.mode("append").parquet(output_path)
else:
    final_sessions_df.checkpoint()
    final_sessions_df.write.mode("append").parquet(output_path)

final_sessions_df.printSchema()