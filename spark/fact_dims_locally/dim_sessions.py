import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, hash, from_unixtime, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from data_schemas import schema

spark = SparkSession.builder \
    .appName("Session Dimension") \
    .master("local[*]") \
    .getOrCreate()

checkpoint_dir = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/checkpoints/sessions_dim/"
spark.sparkContext.setCheckpointDir(checkpoint_dir)

auth_events_schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", LongType(), True),
    StructField("userId", LongType(), True),
    StructField("success", StringType(), True)
])

auth_events_df = spark.read.option("mergeSchema", "true").schema(schema['auth_events']).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_auth_events")

dimsessions_df = auth_events_df.groupBy("sessionId").agg(
    min(from_unixtime(col("ts") / 1000).cast("timestamp")).alias("startTime"),
    max(from_unixtime(col("ts") / 1000).cast("timestamp")).alias("endTime")
).select(
    col("sessionId"),
    col("startTime").cast("string"),
    col("endTime").cast("string")
)

dimsessions_df.checkpoint()

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/session_dimension"
dimsessions_df.write.mode("overwrite").parquet(output_path)

# dimsessions_df.show()