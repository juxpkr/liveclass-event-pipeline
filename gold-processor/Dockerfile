FROM python:3.10-slim-bookworm

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=/usr/local/bin/python

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Build time에 필요한 JAR 미리 다운로드
RUN python -c "from pyspark.sql import SparkSession; \
    spark = SparkSession.builder \
    .appName('WarmupGoldProcessor') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-core_2.12:2.4.0') \
    .getOrCreate(); \
    spark.stop()"

COPY . .

CMD ["python", "-u", "main.py"]