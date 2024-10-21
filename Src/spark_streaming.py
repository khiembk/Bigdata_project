from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .getOrCreate()

# Create a DataFrame representing the stream of input lines from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "your-topic") \
    .load()

# Select the value field and convert it to string
lines = kafka_stream.selectExpr("CAST(value AS STRING)")

# Process the data (e.g., split into words)
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Write the results to HDFS
query = words.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/opt/hadoop/dfs/spark/kafka-output") \
    .option("checkpointLocation", "/opt/hadoop/dfs/spark/checkpoints") \
    .start()

query.awaitTermination()