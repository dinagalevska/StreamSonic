import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, hash, year, month, dayofmonth,
    hour, minute, second, from_unixtime
)
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("Datetime Dimension") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/datetime_dim/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

datetime_dim_schema = StructType([
    StructField("datetimeId", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("ts", LongType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", StringType(), True),
    StructField("second", StringType(), True),
])

raw_events_df = spark.read.option("mergeSchema", "true").parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_page_view_events")

datetime_data_df = raw_events_df.select("ts") \
    .dropDuplicates(["ts"]) \
    .withColumn("timestamp", from_unixtime(col("ts") / 1000).cast("timestamp")) \
    .withColumn("year", year("timestamp")) \
    .withColumn("month", month("timestamp")) \
    .withColumn("day", dayofmonth("timestamp")) \
    .withColumn("hour", hour("timestamp")) \
    .withColumn("minute", minute("timestamp").cast("string")) \
    .withColumn("second", second("timestamp").cast("string")) \
    .withColumn(
        "datetimeId",
        hash(
            concat_ws(
                "_",
                col("year").cast("string"),
                col("month").cast("string"),
                col("day").cast("string"),
                col("hour").cast("string"),
                col("minute"),
                col("second")
            )
        ).cast("long")
    )

final_datetime_dim_df = datetime_data_df.select(
    "datetimeId", "timestamp", "ts", "year", "month", "day", "hour", "minute", "second"
).dropDuplicates(["datetimeId"])

final_datetime_dim_df.checkpoint()

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/datetime_dimension"
final_datetime_dim_df.write.mode("append").parquet(output_path)

# Uncomment the next line to preview the final DataFrame
# final_datetime_dim_df.show()
