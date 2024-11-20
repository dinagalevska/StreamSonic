import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, hash, year, month, dayofmonth,
    hour, minute, second, from_unixtime, when, first, sum, lag, lit, lead
)
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, TimestampType

def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("Datetime Dimension") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/datetime_dim/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/datetime_dimension"

raw_events_df = spark.read.option("mergeSchema", "true").parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_page_view_events")

datetime_data_df = raw_events_df.select("ts") \
    .dropDuplicates(["ts"]) \
    .withColumn("timestamp", from_unixtime(col("ts") / 1000).cast("timestamp")) \
    .withColumn("year", year("timestamp")) \
    .withColumn("month", month("timestamp")) \
    .withColumn("day", dayofmonth("timestamp")) \
    .withColumn("hour", hour("timestamp")) \
    .withColumn("minute", minute("timestamp")) \
    .withColumn("second", second("timestamp")) \
    .withColumn(
        "datetimeId",
        hash(
            concat_ws(
                "_",
                col("year").cast("string"),
                col("month").cast("string"),
                col("day").cast("string"),
                col("hour").cast("string"),
                col("minute").cast("string"),
                col("second").cast("string")
            )
        ).cast("long")
    )


window_spec = Window.partitionBy("datetimeId").orderBy("timestamp")

datetime_changes_df = datetime_data_df.withColumn(
    "prevTimestamp", lag("timestamp", 1).over(window_spec)
).withColumn(
    "isTimestampChanged",
    when(col("prevTimestamp").isNull(), lit(1)) 
    .when(col("prevTimestamp") != col("timestamp"), lit(1))  
    .otherwise(lit(0))
)

grouped_df = datetime_changes_df.withColumn(
    "grouped", sum("isTimestampChanged").over(window_spec)
)

activation_df = grouped_df.groupBy(
    "datetimeId", "timestamp", "year", "month", "day", "hour", "minute", "second"
).agg(
    first("timestamp").alias("rowActivationDate") 
)

window_group_spec = Window.partitionBy("datetimeId").orderBy("rowActivationDate")

final_datetime_dim_df = activation_df.withColumn(
    "rowExpirationDate",
    lead("rowActivationDate", 1).over(window_group_spec)
).withColumn(
    "rowExpirationDate",
    when(col("rowExpirationDate").isNull(), lit(datetime(9999, 12, 31)))  
    .otherwise(col("rowExpirationDate"))
).withColumn(
    "currRow",
    when(col("rowExpirationDate") == lit(datetime(9999, 12, 31)), lit(1)).otherwise(lit(0))  
)

final_datetime_dim_df = final_datetime_dim_df.dropDuplicates(["datetimeId", "rowActivationDate"])

final_datetime_dim_df = final_datetime_dim_df.filter(col("currRow") == 1)


if table_exists(output_path):
    existing_datetime_dim_df = spark.read.parquet(output_path)

    new_records_df = final_datetime_dim_df.join(existing_datetime_dim_df, on=["datetimeId"], how="left_anti")

    new_records_df.checkpoint()

    new_records_df.write.mode("append").parquet(output_path)

else:
    final_datetime_dim_df.checkpoint()
    final_datetime_dim_df.write.mode("append").parquet(output_path)

final_datetime_dim_df.printSchema()