import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, col, current_timestamp, from_json, lit, to_date,
    to_timestamp, unix_timestamp, when,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 강의 메타데이터를 Producer 이벤트에 포함하지 않고 여기서 join
# Producer가 dimension 데이터를 직접 포함해서 보내면 스키마 변경 시 Producer도 함께 수정해야 함
COURSE_DIMENSION = [
    {
        "course_id": "course_001",
        "course_name": "퇴근 후 30일, SQL로 시작하는 데이터 분석",
        "category": "Data",
        "creator_id": "creator_001",
        "creator_name": "데이터핏",
    },
    {
        "course_id": "course_002",
        "course_name": "실무자를 위한 노션 자동화 워크플로우",
        "category": "Productivity",
        "creator_id": "creator_002",
        "creator_name": "일잘러랩",
    },
    {
        "course_id": "course_003",
        "course_name": "처음 시작하는 생성형 AI 콘텐츠 제작",
        "category": "AI",
        "creator_id": "creator_003",
        "creator_name": "프롬프트하우스",
    },
    {
        "course_id": "course_004",
        "course_name": "월 매출 100만원을 만드는 스마트스토어 입문",
        "category": "Business",
        "creator_id": "creator_004",
        "creator_name": "커머스스쿨",
    },
]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "liveclass-events")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://warehouse/bronze/events")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://warehouse/silver/events")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://warehouse/checkpoints/consumer")

VALID_EVENT_TYPES = ["class_view", "video_play", "video_complete", "purchase"]


def create_spark_session():
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "io.delta:delta-core_2.12:2.4.0",
    ]
    spark = (
        SparkSession.builder
        .appName("LiveClassEventConsumer")

        # 로컬 과제 환경에서 두 개의 Spark 프로세스가 동시에 실행됨
        # 각 프로세스가 모든 CPU를 점유하지 않도록 제한
        .master("local[2]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # 데이터 규모가 작으므로 셔플 파티션 최소화 (기본값 200 -> 4)
        .config("spark.sql.shuffle.partitions", "4")
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
        StructField("price", IntegerType(), True),
        StructField("video_position_sec", IntegerType(), True),
        StructField("video_duration_sec", IntegerType(), True),
        StructField("device_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("producer_time", StringType(), True),
    ])


def make_process_batch(schema, course_dim_df):
    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        logger.info(f"Processing Batch {batch_id}...")

        # 원본 데이터 백업 (Bronze)
        # 향후 파싱 로직 변경이나 스키마 에러 발생 시 여기서부터 재처리하기 위함
        bronze_df = batch_df.select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("message_key"),
            col("raw_payload"),
            current_timestamp().alias("ingested_at"),
        )
        bronze_df.write.format("delta").mode("append").save(BRONZE_PATH)

        # JSON 파싱 → dimension join → 품질 판정 순으로 처리
        parsed_df = (
            batch_df
            .select(
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp"),
                col("key").cast("string").alias("message_key"),
                from_json(col("raw_payload"), schema).alias("d"),
            )
            .select(
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "message_key",
                "d.*",
            )
            # 강의 메타데이터 Broadcast Join (데이터 사이즈가 작아 셔플 방지 가능)
            .join(broadcast(course_dim_df), on="course_id", how="left")
            .withColumn("event_ts", to_timestamp(col("event_time")))
            .withColumn("producer_ts", to_timestamp(col("producer_time")))
            .withColumn("processing_time", current_timestamp())
        )
        # DQ 체크 로직: 여기서 필터링해서 버리지 않고 status를 달아서 Silver에 적재
        invalid_condition = (
            col("event_id").isNull()
            | col("event_type").isNull()
            | col("user_id").isNull()
            | col("course_id").isNull()
            | col("course_name").isNull() # Dimension 매핑 실패 케이스
            | col("event_ts").isNull()
            | (~col("event_type").isin(VALID_EVENT_TYPES))
            | ((col("event_type") == "purchase") & col("price").isNull())
        )

        silver_df = (
            parsed_df
            .withColumn("event_date", to_date(col("event_ts")))
            .withColumn(
                "latency_sec",
                unix_timestamp(col("processing_time")) - unix_timestamp(col("event_ts")),
            )
            .withColumn(
                "event_quality_status",
                when(invalid_condition, lit("invalid"))
                .when(col("latency_sec") > 120, lit("late_arrival"))
                .otherwise(lit("valid")),
            )
        )
        (
            silver_df.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            .save(SILVER_PATH)
        )

        logger.info(f"Batch {batch_id} processed successfully.")

    return process_batch


def main():
    logger.info("Starting LiveClass Event Consumer Pipeline...")
    spark = create_spark_session()
    schema = get_event_schema()
    course_dim_df = spark.createDataFrame(COURSE_DIMENSION)

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)

        # 첫 실행 시 토픽에 이미 쌓여 있는 이벤트도 읽기 위해 earliest 사용
        # checkpoint가 존재하면 Spark는 checkpoint에 저장된 offset부터 이어서 처리
        .option("startingOffsets", "earliest")
        # Kafka offset이 만료됐을 때 스트림 전체가 중단되지 않도록 함
        .option("failOnDataLoss", "false")
        .load()
        .withColumn("raw_payload", col("value").cast("string"))
    )

    query = (
        raw_df.writeStream
        .foreachBatch(make_process_batch(schema, course_dim_df))
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
