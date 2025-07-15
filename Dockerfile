FROM openjdk:11-slim

# Set working directory
WORKDIR /app

# Install Python & dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget curl unzip procps && \
    apt-get clean

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Apache Spark manually
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Copy Spark job and config
COPY spark_job.py .
COPY .env .

# Default command to run Spark job
ENTRYPOINT ["spark-submit", "--jars", "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar", "spark_job.py"]
