import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from datetime import datetime

APP_NAME = os.getenv("APP_NAME", "DateTime Dimension")
TEMP_GCS_BUCKET = os.getenv("TEMP_GCS_BUCKET", "streamsonic_bucket")

spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("fs.gs.auth.service.account.enable", "true")
    .config("spark.history.fs.update.interval", "10s")
    .config("temporaryGcsBucket", TEMP_GCS_BUCKET)
    .getOrCreate()
)

startdate = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
enddate = datetime.strptime("2024-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")

df_ref_datetime = spark.range(int((enddate - startdate).total_seconds() / 3600) + 1) \
    .selectExpr(f"CAST({int(startdate.timestamp())} + id * 3600 AS TIMESTAMP) as datetime")

column_rules = [
    ("Year", "year(datetime)"),
    ("Quarter", "quarter(datetime)"),
    ("Month", "month(datetime)"),
    ("Day", "day(datetime)"),
    ("Week", "weekofyear(datetime)"),
    ("QuarterNameLong", "date_format(datetime, 'QQQQ')"),
    ("QuarterNameShort", "date_format(datetime, 'QQQ')"),
    ("QuarterNumberString", "date_format(datetime, 'QQ')"),
    ("MonthNameLong", "date_format(datetime, 'MMMM')"),
    ("MonthNameShort", "date_format(datetime, 'MMM')"),
    ("MonthNumberString", "date_format(datetime, 'MM')"),
    ("DayNumberString", "date_format(datetime, 'dd')"),
    ("WeekNameLong", "concat('week', lpad(weekofyear(datetime), 2, '0'))"),
    ("WeekNameShort", "concat('w', lpad(weekofyear(datetime), 2, '0'))"),
    ("WeekNumberString", "lpad(weekofyear(datetime), 2, '0')"),
    ("DayOfWeek", "dayofweek(datetime)"),
    ("YearMonthString", "date_format(datetime, 'yyyy/MM')"),
    ("DayOfWeekNameLong", "date_format(datetime, 'EEEE')"),
    ("DayOfWeekNameShort", "date_format(datetime, 'EEE')"),
    ("DayOfMonth", "cast(date_format(datetime, 'd') as int)"),
    ("DayOfYear", "cast(date_format(datetime, 'D') as int)"),
    ("Hour", "hour(datetime)"),
    ("Minute", "minute(datetime)"),
    ("Second", "second(datetime)"),
]

for new_column_name, expression in column_rules:
    df_ref_datetime = df_ref_datetime.withColumn(new_column_name, expr(expression))

df_ref_datetime = df_ref_datetime.withColumn(
    "DateKey",
    ((col("Year") * 1000000 + col("Month") * 10000 + col("Day") * 100 + col("Hour")).cast("long"))
)

output_path = "gs://streamsonic_bucket/output/datetime_dimension"
checkpoint_dir = "gs://streamsonic_bucket/checkpoints/datetime_dimension_checkpoint/"

df_ref_datetime.write \
    .format("bigquery") \
    .option("table", "streamsonic-441414:streamsonic_dataset.dim_datetime") \
    .option("checkpointLocation", checkpoint_dir) \
    .mode("append") \
    .save()

spark.stop()
