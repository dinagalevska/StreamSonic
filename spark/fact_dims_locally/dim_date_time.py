from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sequence, to_timestamp
from pyspark.sql.types import TimestampType
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DateTime Dimension") \
    .getOrCreate()

# Define start and end dates as timestamps
startdate = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
enddate = datetime.strptime("2024-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")

# Generate a range of timestamps (by hour) using Spark SQL sequence function
df_ref_datetime = spark.range(int((enddate - startdate).total_seconds() / 3600) + 1) \
    .selectExpr(f"CAST({int(startdate.timestamp())} + id * 3600 AS TIMESTAMP) as datetime")

# Define column transformations
column_rules = [
    ("DateSK", "cast(date_format(datetime, 'yyyyMMdd') as int)"),
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

# Apply all column transformations
for new_column_name, expression in column_rules:
    df_ref_datetime = df_ref_datetime.withColumn(new_column_name, expr(expression))

# Define the output path for saving the data
output_path = "/mnt/c/Users/Dina Galevska/streamSonic/StreamSonic/dim_fact_tables_locally/datetime_dimension_with_time"

# Save the resulting dataframe as Parquet (or another format)
df_ref_datetime.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
