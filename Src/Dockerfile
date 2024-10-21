FROM bde2020/spark-worker:3.0.0-hadoop3.2

# Install build dependencies, Python dev headers, and Fortran compiler
RUN apk add --no-cache \
    build-base \
    python3 \
    py3-pip \
    python3-dev \
    gfortran \
    && python3 -m pip install --upgrade pip

# Install Python packages
RUN python3 -m pip install --no-cache-dir numpy pyspark kafka-python

# Copy your consumer script into the container
COPY consumer.py /app/consumer.py

# Set the working directory
WORKDIR /app

# Command to run your consumer
CMD ["python3", "consumer.py"]
