# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import json
import sys

def create_spark_session():
    """Khởi tạo Spark Session"""
    return SparkSession.builder \
        .appName("{} Stock Analysis".format(sys.argv[1]) if len(sys.argv) > 1 else "Stock Analysis") \
        .getOrCreate()

def create_schema():
    """Định nghĩa schema cho dữ liệu"""
    return StructType([
        StructField("Ticker", StringType(), True),
        StructField("Date", DateType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True),
        StructField("Dividends", DoubleType(), True),
        StructField("Stock Splits", DoubleType(), True)
    ])


def calculate_basic_stats(df):
    """Tính toán các chỉ số thống kê cơ bản"""
    stats = df.agg(
        mean('Close').alias('avg_close'),
        max('High').alias('highest_price'),
        min('Low').alias('lowest_price'),
        mean('Volume').alias('avg_volume'),
        sum('Volume').alias('total_volume')
    )
    return stats

def calculate_daily_returns(df):
    """Tính toán tỷ lệ thay đổi giá hàng ngày"""
    window = Window.orderBy('Date')
    return df.withColumn('prev_close', lag('Close').over(window)) \
             .withColumn('daily_return', ((col('Close') - col('prev_close')) / col('prev_close')) * 100)

def calculate_bollinger_bands(df, window_size=20, std_dev=2):
    """Tính toán Bollinger Bands"""
    window = Window.orderBy('Date').rowsBetween(-(window_size-1), 0)
    
    return df.withColumn('BB_middle', mean('Close').over(window)) \
            .withColumn('BB_std', stddev('Close').over(window)) \
            .withColumn('BB_upper', col('BB_middle') + (col('BB_std') * std_dev)) \
            .withColumn('BB_lower', col('BB_middle') - (col('BB_std') * std_dev))

def calculate_rsi(df, periods=14):
    """Tính toán RSI"""
    window = Window.orderBy('Date')
    
    # Tính price change
    df = df.withColumn('price_change', col('Close') - lag('Close', 1).over(window))
    
    # Tính gain và loss
    df = df.withColumn('gain', when(col('price_change') > 0, col('price_change')).otherwise(0)) \
           .withColumn('loss', when(col('price_change') < 0, -col('price_change')).otherwise(0))
    
    # Tính average gain và loss
    window_rsi = Window.orderBy('Date').rowsBetween(-(periods-1), 0)
    df = df.withColumn('avg_gain', mean('gain').over(window_rsi)) \
           .withColumn('avg_loss', mean('loss').over(window_rsi))
    
    # Tính RSI
    return df.withColumn('RSI', 100 - (100 / (1 + (col('avg_gain') / col('avg_loss')))))

def analyze_trend(df):
    """Phân tích xu hướng"""
    window5 = Window.orderBy('Date').rowsBetween(-4, 0)
    window20 = Window.orderBy('Date').rowsBetween(-19, 0)
    
    df = df.withColumn('SMA_5', mean('Close').over(window5)) \
           .withColumn('SMA_20', mean('Close').over(window20))
    
    df = df.withColumn('Trend', 
        when(col('SMA_5') > col('SMA_20'), 'Uptrend')
        .when(col('SMA_5') < col('SMA_20'), 'Downtrend')
        .otherwise('Sideways'))
    
    # Tính Momentum
    window_momentum = Window.orderBy('Date')
    df = df.withColumn('Momentum', 
                      col('Close') - lag('Close', 5).over(window_momentum))
    
    return df

def analyze_stock_data(spark, df):
    """Hàm chính để phân tích dữ liệu"""
    
    # Thực hiện các phân tích
    df = calculate_daily_returns(df)
    df = calculate_bollinger_bands(df)
    df = calculate_rsi(df)
    df = analyze_trend(df)
    
    # Tính toán thống kê cơ bản
    basic_stats = calculate_basic_stats(df)
    
    # Lấy các chỉ số mới nhất
    latest_data = df.orderBy(desc('Date')).first()
    
    # Tạo dictionary chứa kết quả phân tích
    analysis_results = {
        'Basic_stats': {
            'Average_close_price': float(basic_stats.collect()[0]['avg_close']),
            'Highest_price': float(basic_stats.collect()[0]['highest_price']),
            'Lowest_price': float(basic_stats.collect()[0]['lowest_price']),
            'Average_trade_volume': float(basic_stats.collect()[0]['avg_volume']),
            'Total_trade_volume': float(basic_stats.collect()[0]['total_volume'])
        },
        # 'Chỉ số kỹ thuật hiện tại': {
        'Technical_indicators': {
            'RSI': float(latest_data['RSI']) if latest_data['RSI'] else None,
            'Bollinger_Middle': float(latest_data['BB_middle']) if latest_data['BB_middle'] else None,
            'Bollinger_Upper': float(latest_data['BB_upper']) if latest_data['BB_upper'] else None,
            'Bollinger_Lower': float(latest_data['BB_lower']) if latest_data['BB_lower'] else None,
            'Trend': latest_data['Trend'],
            'Momentum': float(latest_data['Momentum']) if latest_data['Momentum'] else None
        }
    }
    
    return analysis_results, df

def print_analysis(analysis):
    """In kết quả phân tích"""
    for category, metrics in analysis.items():
        print("\n{}:".format(category))
        for metric, value in metrics.items():
            if isinstance(value, float):
                print("{metric}: {value:.2f}".format(metric=metric, value=value))
            else:
                print("{metric}: {value}".format(metric=metric, value=value))

# Sử dụng code
if __name__ == "__main__":
    # Khởi tạo Spark session
    spark = create_spark_session()
    
    # Dữ liệu
    schema = create_schema()
    
    file_path = "hdfs://namenode:9000/user/hadoop/AAPL_stock_data.json"
    print("Đọc dữ liệu từ file: {}".format(file_path))
    if len(sys.argv) > 1:
        file_path = "hdfs://namenode:9000/user/hadoop/{}_stock_data.json".format(sys.argv[1])
    data = spark.read.option("multiline", "true").schema(schema).json(file_path)
    
    data.show()
    
    # Thực hiện phân tích
    analysis_results, df = analyze_stock_data(spark, data)
    
    # In kết quả
    print_analysis(analysis_results)
    
    output_path = "hdfs://namenode:9000/user/hadoop/output/AAPL_stock_analysis.json"
    if len(sys.argv) > 1:
        output_path = "hdfs://namenode:9000/user/hadoop/output/{}_stock_analysis.json".format(sys.argv[1])

    output_path2 = output_path.replace("analysis", "analysis_table")
    # Lưu kết quả phân tích ra file JSON
    analysis_results_df = spark.createDataFrame([(analysis_results,)], ['Analysis'])
    analysis_results_df.write.mode("overwrite").option("encoding", "UTF-8").json(output_path)
    
    newdf = df.drop('Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits')
    
    newdf.write.mode("overwrite").option("encoding", "UTF-8").json(output_path2)

    
    print("Kết quả phân tích được lưu vào file: {} và {}".format(output_path, output_path2))
    
    # Đóng Spark session
    spark.stop()