from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Define the schema for your JSON data
json_schema = StructType([
    StructField("Ticker", StringType(), True),
    StructField("Date", DateType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Dividends", DoubleType(), True),
    StructField("Stock Splits", DoubleType(), True),
])

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkHDFSExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Convert the Kafka message value from binary to string and parse JSON
messages_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), json_schema).alias("data")) \
    .select("data.*")  # Select fields from the parsed JSON

# Write the messages to HDFS in text format
query = messages_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/kafka_output") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

# Await termination of the stream
query.awaitTermination()
