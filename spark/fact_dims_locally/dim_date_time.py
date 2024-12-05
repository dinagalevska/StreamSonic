import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, hash, year, month, dayofmonth,
    hour, minute, second, from_unixtime, when, first, sum, dayofweek, lit, lead, unix_timestamp
)
from pyspark.sql.window import Window
from datetime import datetime

from data_schemas import schema


def table_exists(path: str) -> bool:
    return os.path.exists(path)

spark = SparkSession.builder \
    .appName("Datetime Dimension") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/datetime_dim/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/datetime_dimension"

# raw_events_df = spark.read.option("mergeSchema", "true").schema(schema["page_view_events"]).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_page_view_events")

raw_events_df = spark.read.option("mergeSchema", "true").schema(schema['listen_events']).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/correct_listen_events")

datetime_data_df = raw_events_df.select("ts")

datetime_data_df = datetime_data_df \
    .withColumn("year", year("ts")) \
    .withColumn("month", month("ts")) \
    .withColumn("day", dayofmonth("ts")) \
    .withColumn("hour", hour("ts")) \
    .withColumn("minute", minute("ts")) \
    .withColumn("second", second("ts")) \
    .withColumn(
        "weekendFlag",
        when(dayofweek("ts").isin(6, 7), lit(True)).otherwise(lit(False))
    ) \
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
                col("second").cast("string"),
            )
        ).cast("long"))
    
datetime_data_df = datetime_data_df.select('ts', 'year', 'month', 'day', 'hour', 'minute', 'second', 'datetimeId', 'weekendFlag') \
    .drop_duplicates(['datetimeId'])

window_spec = Window.partitionBy("datetimeId").orderBy("ts")

datetime_changes_df = datetime_data_df.withColumn(
    "rowActivationDate", col("ts")  
).withColumn(
    "rowExpirationDate", lit(datetime(9999, 12, 31)) 
).withColumn(
    "currRow", lit(1)  
)

window_group_spec = Window.partitionBy("datetimeId").orderBy("rowActivationDate")


final_datetime_dim_df = datetime_changes_df.withColumn(
    "rowExpirationDate",
    when(
        lead("rowActivationDate", 1).over(window_group_spec).isNull(),
        lit(datetime(9999, 12, 31))  
    ).otherwise(lead("rowActivationDate", 1).over(window_group_spec)) 
)


final_datetime_dim_df = final_datetime_dim_df.withColumn(
    "currRow",
    when(col("rowExpirationDate") == lit(datetime(9999, 12, 31)), lit(1)) 
    .otherwise(lit(0))
)
final_datetime_dim_df = final_datetime_dim_df.select(
    'datetimeId',
    'ts',
    'year', 
    'month', 
    'day', 
    'hour', 
    'minute', 
    'second',
    'weekendFlag',
    'rowActivationDate',
    'rowExpirationDate',
    'currRow'
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