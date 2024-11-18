FROM bde2020/spark-worker:3.0.0-hadoop3.2

RUN apk add --no-cache \
    build-base \
    python3 \
    py3-pip \
    python3-dev \
    gfortran \
    && python3 -m pip install --upgrade pip

# Install Python packages
RUN python3 -m pip install --no-cache-dir numpy pyspark kafka-python

# Add Elasticsearch-Hadoop connector
ENV SPARK_JARS_DIR="/spark/jars"
RUN mkdir -p ${SPARK_JARS_DIR} \
    && curl -L -o ${SPARK_JARS_DIR}/elasticsearch-hadoop.jar \
       https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-hadoop/8.16.0/elasticsearch-hadoop-8.16.0.jar

COPY importdata.py /app/importdata.py

# Set the working directory
WORKDIR /app

# Command to run your consumer
CMD spark-submit \
    --master spark://spark-master:7077 \
    --jars /spark/jars/elasticsearch-hadoop.jar \
    /app/importdata.py