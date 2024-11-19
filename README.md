# Bigdata_project

docker-compose up --build -d

## Run processing code in spark

/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /process_data.py [Tên_cổ_phiếu]

- Mặc định data được lấy xử lý được lưu ở hdfs://namenode:9000/user/hadoop/{Tên_cổ_phiếu}_stock_data.json
- Dữ liệu phân tích xử lý xong được lưu ở hdfs://namenode:9000/user/hadoop/output/{Tên_cổ_phiếu}_stock_analysis.json
