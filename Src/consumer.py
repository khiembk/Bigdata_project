from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the schema for your JSON data
json_schema = StructType([
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

# Write the messages to HDFS in text format
query = kafka_df.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("text") \
    .option("path", "hdfs://namenode:9000/kafka_output") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start() \
    .awaitTermination()
