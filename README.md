# Bigdata_project

docker-compose up --build -d

## Run processing code in spark

/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /process_data.py [Tên_cổ phiếu]
