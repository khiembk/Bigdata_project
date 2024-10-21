from pyspark.sql import SparkSession

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

# Convert the Kafka message value from binary to string
messages_df = kafka_df.selectExpr("CAST(value AS STRING) as message")

# Write the messages to HDFS in text format
query = messages_df.writeStream \
    .outputMode("append") \
    .format("text") \
    .option("path", "hdfs://namenode:9000/kafka_output") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

# Await termination of the stream
query.awaitTermination()
