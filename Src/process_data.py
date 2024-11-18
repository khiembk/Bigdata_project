# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Process JSON Data") \
    .getOrCreate()
import sys



# Định nghĩa schema
schema = StructType([
    StructField("Ticker", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Dividends", DoubleType(), True),
    StructField("Stock Splits", DoubleType(), True)
])

# Đọc tệp JSON với schema
file_path = "hdfs://namenode:9000/user/hadoop/AAPL_stock_data.jsonl"

if len(sys.argv) > 1:
    file_path = "hdfs://namenode:9000/user/hadoop/{}_stock_data.jsonl".format(sys.argv[1])
    
print("Đọc dữ liệu từ file: {}".format(file_path))
df = spark.read.schema(schema).json(file_path)


# Hiển thị dữ liệu ban đầu
print("Dữ liệu ban đầu:")
df.show()

# Kiểm tra schema
print("Schema của dữ liệu:")
df.printSchema()

# Tính giá trị trung bình, cao nhất, thấp nhất của cột "Close" theo từng ticker
agg_df = df.groupBy("Ticker").agg(
    avg("Close").alias("Avg_Close"),
    max("Close").alias("Max_Close"),
    min("Close").alias("Min_Close")
)

# Hiển thị kết quả
print("Thống kê dữ liệu:")
print(agg_df.show())

# Lưu kết quả ra file JSON trên HDFS
output_path = "hdfs://namenode:9000/user/hadoop/output/processed_AAPL_data.jsonl"
if len(sys.argv) > 1:
    output_path = "hdfs://namenode:9000/user/hadoop/output/processed_{}_data.jsonl".format(sys.argv[1])

agg_df.write.json(output_path)

print("Lưu kết quả vào file: {}".format(output_path))
# Dừng SparkSession
spark.stop()
