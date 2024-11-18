from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, lag, lit, when, rank, sum as _sum, first
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from data_schemas import schema

user_dim_schema = StructType([
    StructField("userId", IntegerType(), False),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("level", StringType(), True),
    StructField("registration", IntegerType(), True),
    StructField("eventTimestamp", TimestampType(), True),
    StructField("rowActivationDate", TimestampType(), True),
    StructField("rowExpirationDate", TimestampType(), True),
    StructField("currRow", IntegerType(), True)
])


spark = SparkSession.builder \
    .appName("SCD Type 2 for User Dimension") \
    .master("local[*]") \
    .getOrCreate()

listen_events_df = spark.read.option("mergeSchema", "true").schema(schema['listen_events']).parquet("/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/tmp/raw_listen_events")

user_base_df = listen_events_df.select(
    col("userId").cast("bigint"),
    col("firstName"),
    col("lastName"),
    col("gender"),
    col("level"),
    (col("ts") / 1000).cast("timestamp").alias("eventTimestamp"),
    col("registration").cast("bigint")
).distinct()

window_spec = Window.partitionBy("userId").orderBy("eventTimestamp")

user_changes_df = user_base_df.withColumn(
    "prevLevel",
    lag("level", 1).over(window_spec)
).withColumn(
    "isLevelChanged",
    when(col("prevLevel").isNull(), lit(1))  
    .when(col("prevLevel") != col("level"), lit(1))  
    .otherwise(lit(0))
)

grouped_df = user_changes_df.withColumn(
    "grouped",
    _sum("isLevelChanged").over(window_spec)
)


activation_df = grouped_df.groupBy(
    "userId", "firstName", "lastName", "gender"
).agg(
    first("level").alias("level"),
    first("registration").alias("registration"),
    first("grouped").alias("grouped"),
    first("eventTimestamp").alias("rowActivationDate") 
)

window_group_spec = Window.partitionBy("userId", "firstName", "lastName").orderBy("rowActivationDate")

final_user_dim_df = activation_df.withColumn(
    "rowExpirationDate",
    lead("rowActivationDate", 1).over(window_group_spec)
).withColumn(
    "rowExpirationDate",
    when(col("rowExpirationDate").isNull(), lit(datetime(9999, 12, 31))).otherwise(col("rowExpirationDate"))
).withColumn(
    "currRow",
    when(col("rowExpirationDate") == lit(datetime(9999, 12, 31)), lit(1)).otherwise(lit(0))
)

output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/user_dimension"
final_user_dim_df.write.mode("append").parquet(output_path)

spark.read.parquet(output_path).show()
