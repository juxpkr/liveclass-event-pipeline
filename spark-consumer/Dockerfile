FROM python:3.10-slim-bookworm

# 필수 환경변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=/usr/local/bin/python

WORKDIR /app

# Java 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# PySpark 및 파이썬 라이브러리 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 도커 이미지 빌드 타임에 JAR 파일들을 미리 다운로드
# 임시로 SparkSession을 띄워 설정된 패키지들을 다운로드하게 한 뒤 바로 종료
RUN python -c "from pyspark.sql import SparkSession; \
    spark = SparkSession.builder \
    .appName('PreloadSparkJars') \
    .master('local[*]') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-core_2.12:2.4.0') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate(); \
    spark.stop()"

COPY consumer.py .

# 실행 (spark-submit 대신 python으로 직접 실행해도 PySpark가 동일하게 처리)
CMD ["python", "-u", "consumer.py"]