from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType\

# Define Spark session with Elasticsearch configuration
spark = SparkSession.builder \
    .appName("HadoopToElasticSearch") \
    .master("spark://172.19.0.10:7077") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "stock-index/_doc") \
    .getOrCreate()

# Read data from HDFS
hdfs_path = "hdfs://namenode:9000/stock_data_done/"

schema = StructType([
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
stock_data = spark.read.schema(schema).json(hdfs_path)

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