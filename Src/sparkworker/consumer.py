from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define Spark session
spark = SparkSession.builder \
    .appName("StockDataKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Consume stock data message
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

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-data") \
    .load()
    
# Add a date column to partition by
kafka_df = kafka_df.withColumn("date", date_format(current_date(), "yyyy/MM/dd"))
query = kafka_df.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("text") \
    .option("path", "hdfs://namenode:9000/stock_data") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .partitionBy("date") \
    .start() \
    .awaitTermination()
    
# Consume job type message
def merge_daily_files(spark, date_to_merge):
    input_path = f"hdfs://namenode:9000/stock_data/{date_to_merge}"
    output_path = f"hdfs://namenode:9000/stock_data_done/{date_to_merge}.json"
    
    # Read all small files from that day's directory
    df = spark.read.text(input_path)
    
    # Merge and write to a single file
    df.coalesce(1).write.mode("overwrite").json(output_path)
    
    print(f"Data for {date_to_merge} has been merged and saved to {output_path}")

command_schema = StructType().add("command", StringType()).add("date", StringType())

kafka_df_command = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "merge-commands") \
    .load()

parsed_df_command = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), command_schema).alias("data")) \
    .select("data.command", "data.date")

# Filter for the "merge" command only
merge_requests_df = parsed_df_command.filter(col("command") == "merge")

# Function to process each batch of merge requests
def process_merge_requests(batch_df, batch_id):
    for row in batch_df.collect():
        date_to_merge = row['date']
        merge_daily_files(spark, date_to_merge)

# Start processing merge requests
query_merge = merge_requests_df.writeStream \
    .outputMode("update") \
    .foreachBatch(process_merge_requests) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_merge") \
    .start()