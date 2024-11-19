from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Define Spark session with Elasticsearch configuration
spark = SparkSession.builder \
    .appName("HadoopToElasticSearch") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "stock-index/_doc") \
    .getOrCreate()

# Read data from HDFS
hdfs_path = "hdfs://namenode:9000/stock_data_done/"
stock_data = spark.read.json(hdfs_path)

# Transform data if needed (e.g., rename fields for Elasticsearch compatibility)
transformed_data = stock_data.select(
    col("Ticker").alias("ticker"),
    col("Date").alias("date"),
    col("Open").alias("open"),
    col("High").alias("high"),
    col("Low").alias("low"),
    col("Close").alias("close"),
    col("Volume").alias("volume"),
    col("Dividends").alias("dividends"),
    col("Stock Splits").alias("stock_splits")
)

transformed_data = stock_data.withColumn(
    "ticker_date", 
    concat_ws("_", col("Ticker"), col("Date"))
)

# Write data to Elasticsearch
transformed_data.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock-index/_doc") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.mapping.id", "ticker_date") \
    .mode("append") \
    .save()