# Bigdata_project

- docker-compose up --build -d
- Chạy file producer.py (DATA cổ phiếu là AAPL_stock_data.json, dùng cổ phiếu khác thì crawl về và đổi tên)
- Vào container spark consumer, chạy lệnh ở (*)
- Vào Kibana tại localhost:5601, chọn index -> thực hiện vẽ đồ thị

## Run processing code in spark

/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client app/process_data.py [Tên_cổ_phiếu]

- Mặc định data được lấy xử lý được lưu ở hdfs://namenode:9000/user/hadoop/{Tên_cổ_phiếu}_stock_data.json
- Dữ liệu tổng kết phân tích xử lý xong được lưu ở hdfs://namenode:9000/user/hadoop/output/{Tên_cổ_phiếu}_stock_analysis.json
- Dữ liệu phân tích theo từng ngày được lưu ở hdfs://namenode:9000/user/hadoop/output/{Tên_cổ_phiếu}_stock_analysis_table.json

## Thêm dữ liệu vào elastic search (*)
- cd /
- ./spark/bin/spark-submit --master spark://spark-master:7077 --jars app/elasticsearch-spark-30_2.12-7.17.13.jar --driver-class-path app/elasticsearch-spark-30_2.12-7.17.13.jar app/importdata.py

## Các trang
- Kibana: localhost:5601
- Hadoop: localhost:9870