from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, year, udf


@udf
def string_decode(s, encoding='utf-8'):
    if s:
        return (s.encode('latin1')        
                .decode('unicode-escape') 
                .encode('latin1')        
                .decode(encoding)        
                .strip('\"'))
    else:
        return s

#local[*]
def create_or_get_spark_session(app_name, master="yarn"):
    """
    Creates or gets a Spark Session

    Parameters:
        app_name : str
            Pass the name of your app
        master : str
            Choosing the Spark master, local is the default
    Returns:
        spark: SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config(master=master) \
        .getOrCreate()

    return spark



def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset="earliest"):
    """
    Creates a kafka read stream

    Parameters:
        spark : SparkSession
            A SparkSession object
        kafka_address: str
            Host address of the kafka bootstrap server
        topic : str
            Name of the kafka topic
        starting_offset: str
            Starting offset configuration, "earliest" by default 
    Returns:
        read_stream: DataStreamReader
    """

    read_stream = (spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port}")  # Parameterized
                   .option("failOnDataLoss", False)
                   .option("startingOffsets", starting_offset)
                   .option("subscribe", topic)
                   .load())

    return read_stream


def process_stream(stream, stream_schema, topic):
    """
    Process stream to fetch on value from the kafka message.
    convert ts to timestamp format and produce year, month, day,
    hour columns
    Parameters:
        stream : DataStreamReader
            The data stream reader for your stream
    Returns:
        stream: DataStreamReader
    """

    stream = (stream
              .selectExpr("CAST(value AS STRING) AS value")
              .select(
                  from_json(col("value"), stream_schema).alias("data")
              )
              .select("data.*")
              )

    # # Add month, day, hour to split the data into separate directories
    # stream = (stream
    #           .withColumn("ts", (col("ts") / 1000).cast("timestamp"))
    #           .withColumn("year", year(col("ts")))
    #           .withColumn("month", month(col("ts")))
    #           .withColumn("hour", hour(col("ts")))
    #           .withColumn("day", dayofmonth(col("ts")))
    #           )

    if topic in ["listening_events_schema", "page_view_events_schema"]:
        stream = (stream
                  .withColumn("song", string_decode("song"))
                  .withColumn("artist", string_decode("artist"))
                  )

    return stream


def create_file_write_stream(stream, storage_path, checkpoint_path, trigger="10 seconds", output_mode="append", file_format="parquet"):
    """
    Write the stream back to a file store

    Parameters:
        stream : DataStreamReader
            The data stream reader for your stream
        file_format : str
            parquet, csv, orc etc
        storage_path : str
            The file output path
        checkpoint_path : str
            The checkpoint location for spark
        trigger : str
            The trigger interval
        output_mode : str
            append, complete, update
    """

    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    # .partitionBy("month", "day", "hour")
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream