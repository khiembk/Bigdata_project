from threading import Thread
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("MergeCommandProcessor")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockDataKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Schema for stock data
stock_data_schema = StructType([
    StructField("Ticker", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Dividends", DoubleType(), True),
    StructField("Stock Splits", DoubleType(), True),
])

# Schema for commands
command_schema = StructType().add("command", StringType()).add("date", StringType())

# Function to process stock data stream
def process_stock_data():
    logger.info("Starting stock data query...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "stock-data") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), stock_data_schema).alias("data")) \
        .select("data.*") \
        
    processed_stream = parsed_df \
        .withColumn("year", date_format(to_date(col("Date"), "yyyy-MM-dd"), "yyyy")) \
        .withColumn("month", date_format(to_date(col("Date"), "yyyy-MM-dd"), "MM")) \
        .withColumn("day", date_format(to_date(col("Date"), "yyyy-MM-dd"), "dd"))

    query = processed_stream.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/stock_data") \
        .option("checkpointLocation", "/tmp/spark_checkpoint_stock") \
        .partitionBy("year", "month", "day") \
        .start()

    query.awaitTermination()

# Function to process merge commands
def process_merge_commands():
    logger.info("Starting merge commands query...")

    kafka_df_command = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "merge-commands") \
        .load()

    parsed_df_command = kafka_df_command.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), command_schema).alias("data")) \
        .select("data.*")

    def process_merge_requests(batch_df, batch_id):
        batch_df.show(truncate=False)
        for row in batch_df.collect():
            date_to_merge = row['date']
            try:
                date_obj = datetime.strptime(date_to_merge, "%Y-%m-%d")
            except ValueError:
                date_obj = datetime.strptime(date_to_merge, "%Y/%m/%d")
            year = str(date_obj.year)
            month = str(date_obj.month).zfill(2)
            day = str(date_obj.day).zfill(2)
            merge_daily_files(spark, year, month, day)

    query_merge = parsed_df_command.writeStream \
        .outputMode("update") \
        .foreachBatch(process_merge_requests) \
        .option("checkpointLocation", "/tmp/spark_checkpoint_merge") \
        .start()

    query_merge.awaitTermination()

# Function to merge daily files
def merge_daily_files(spark, year, month, day):
    try:
        input_path = f"hdfs://namenode:9000/stock_data/year={year}/month={month}/day={day}"
        output_path = f"hdfs://namenode:9000/stock_data_done/{year}/{month}/{day}"
        
        # Read all small files from that day's directory
        df = spark.read.parquet(input_path)
        
        # Merge and write to a single file
        df.coalesce(1).write.mode("overwrite").json(output_path)
        
        logger.info(f"Data for {input_path} has been merged and saved to {output_path}")
    except Exception as e:
        logger.error(f"Error occurred while processing data for {year}-{month}-{day}: {e}")

# Run both queries concurrently
thread1 = Thread(target=process_stock_data)
thread2 = Thread(target=process_merge_commands)

thread1.start()
thread2.start()

thread1.join()
thread2.join()
