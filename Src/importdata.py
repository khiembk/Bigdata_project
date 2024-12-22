from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import Window
from pyspark.sql.functions import col, mean, stddev, lag, when

# Define Spark session with Elasticsearch configuration
spark = SparkSession.builder \
    .appName("HadoopToElasticSearch") \
    .master("spark://spark-master:7077") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "stock-index/_doc") \
    .getOrCreate()

# Read data from HDFS
hdfs_path = "hdfs://namenode:9000/stock_data_done/*/*/*"
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

# Process data functions
def calculate_bollinger_bands(df, window_size=20, std_dev=2):
    """Calculate Bollinger Bands."""
    window = Window.orderBy('Date').rowsBetween(-(window_size-1), 0)
    return df.withColumn('BB_middle', mean('Close').over(window)) \
             .withColumn('BB_std', stddev('Close').over(window)) \
             .withColumn('BB_upper', col('BB_middle') + (col('BB_std') * std_dev)) \
             .withColumn('BB_lower', col('BB_middle') - (col('BB_std') * std_dev))

def calculate_rsi(df, periods=14):
    """Calculate RSI."""
    window = Window.orderBy('Date')
    df = df.withColumn('price_change', col('Close') - lag('Close', 1).over(window))
    df = df.withColumn('gain', when(col('price_change') > 0, col('price_change')).otherwise(0)) \
           .withColumn('loss', when(col('price_change') < 0, -col('price_change')).otherwise(0))
    window_rsi = Window.orderBy('Date').rowsBetween(-(periods-1), 0)
    df = df.withColumn('avg_gain', mean('gain').over(window_rsi)) \
           .withColumn('avg_loss', mean('loss').over(window_rsi))
    return df.withColumn('RSI', 100 - (100 / (1 + (col('avg_gain') / col('avg_loss')))))

def analyze_trend(df):
    """Analyze trend."""
    window5 = Window.orderBy('Date').rowsBetween(-4, 0)
    window20 = Window.orderBy('Date').rowsBetween(-19, 0)
    df = df.withColumn('SMA_5', mean('Close').over(window5)) \
           .withColumn('SMA_20', mean('Close').over(window20))
    df = df.withColumn('Trend', 
                       when(col('SMA_5') > col('SMA_20'), 'Uptrend')
                       .when(col('SMA_5') < col('SMA_20'), 'Downtrend')
                       .otherwise('Sideways'))
    window_momentum = Window.orderBy('Date')
    df = df.withColumn('Momentum', col('Close') - lag('Close', 5).over(window_momentum))
    return df

# Apply transformations
stock_data = calculate_bollinger_bands(stock_data)
stock_data = calculate_rsi(stock_data)
stock_data = analyze_trend(stock_data)

# Show transformed data
stock_data.show()

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
    col("Stock Splits").alias("stock_splits"),
    col("BB_middle"),
    col("BB_std"),
    col("BB_upper"),
    col("BB_lower"),
    col("RSI"),
    col("SMA_5"),
    col("SMA_20"),
    col("Trend"),
    col("Momentum")
)

# Add unique identifier for Elasticsearch
transformed_data = transformed_data.withColumn(
    "ticker_date", 
    concat_ws("_", col("ticker"), col("date"))
)

# Write data to Elasticsearch
transformed_data.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "stock-index/_doc") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.mapping.id", "ticker_date") \
    .mode("append") \
    .save()