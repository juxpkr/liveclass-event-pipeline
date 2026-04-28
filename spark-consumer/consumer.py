import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "liveclass-events")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "s3a://warehouse/events")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://warehouse/checkpoints/events_chk")


def create_spark_session():
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "io.delta:delta-core_2.12:2.4.0",
    ]

    spark = (
        SparkSession.builder
        .appName("LiveClassEventSparkConsumer")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_event_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("user_segment", StringType(), True),
        StructField("course_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("instructor_id", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("video_position_sec", IntegerType(), True),
        StructField("video_duration_sec", IntegerType(), True),
        StructField("device_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("producer_time", StringType(), True),
    ])

def main():
    print("🚀 Starting Spark Structured Streaming consumer...")
    spark = create_spark_session()
    print("✅ Spark session created")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), get_event_schema()).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("producer_time", to_timestamp(col("producer_time")))
    )

    print(f"⏳ Writing stream to Delta Lake: {OUTPUT_PATH}")
    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(OUTPUT_PATH)
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()