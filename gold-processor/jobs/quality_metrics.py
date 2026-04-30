import logging
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, coalesce, col, concat_ws, count, current_timestamp, lit, when, window,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
)

from config import BRONZE_PATH, GOLD_QUALITY_PATH, SILVER_PATH
from spark_session import delta_table_exists

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

WINDOW_SIZE = "30 seconds"


def make_lineage_key():
    return concat_ws(
        "-",
        col("kafka_topic"),
        col("kafka_partition").cast("string"),
        col("kafka_offset").cast("string"),
    )


def build_bronze_lineage(bronze_df):
    # Bronze 기준 메시지 목록. Kafka topic/partition/offset 조합을 lineage key로 사용
    return (
        bronze_df
        .withColumn("lineage_key", make_lineage_key())
        .select("lineage_key", "ingested_at")
        .dropDuplicates(["lineage_key"])
    )


def build_silver_lineage(silver_df):
    # Silver 처리 결과. 같은 lineage key를 가지고 Bronze와 비교
    return (
        silver_df
        .withColumn("lineage_key", make_lineage_key())
        .select(
            "lineage_key",
            "processing_time",
            "event_quality_status",
            "latency_sec",
        )
    )


def build_data_quality_metrics(spark: SparkSession):
    # 현재 과제 환경은 로컬 소규모 데이터이므로 Bronze/Silver 전체를 읽어 정합성 지표를 계산함
    # 운영 환경에서는 매 배치마다 전체 테이블을 스캔하지 않고,
    # Bronze는 최근 몇 분, Silver는 처리 지연을 고려해서 더 넓은 구간을 읽는 lookback window 방식이나 증분 처리로 확장 가능
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    bronze_lineage_df = build_bronze_lineage(bronze_df)
    silver_lineage_df = build_silver_lineage(silver_df)

    bronze_window_df = (
        bronze_lineage_df
        .withColumn("event_window", window(col("ingested_at"), WINDOW_SIZE))
        .select(
            "lineage_key",
            col("event_window.start").alias("window_start"),
            col("event_window.end").alias("window_end"),
        )
    )

    joined_df = (
        bronze_window_df.alias("b")
        .join(silver_lineage_df.alias("s"), on="lineage_key", how="left")
    )

    metrics_df = (
        joined_df
        .groupBy("window_start", "window_end")
        .agg(
            count("lineage_key").alias("total_bronze_messages"),

            spark_sum(
                when(col("processing_time").isNotNull(), 1).otherwise(0)
            ).alias("matched_silver_messages"),

            spark_sum(
                when(col("processing_time").isNull(), 1).otherwise(0)
            ).alias("missing_in_silver_messages"),

            spark_sum(
                when(col("event_quality_status") == "valid", 1).otherwise(0)
            ).alias("valid_events"),

            spark_sum(
                when(col("event_quality_status") == "invalid", 1).otherwise(0)
            ).alias("invalid_events"),

            spark_sum(
                when(col("event_quality_status") == "late_arrival", 1).otherwise(0)
            ).alias("late_arrival_events"),

            avg("latency_sec").alias("avg_latency_sec"),
            spark_max("latency_sec").alias("max_latency_sec"),
        )
    )

    metrics_df = (
        metrics_df
        .withColumn(
            "bronze_to_silver_match_rate",
            when(
                col("total_bronze_messages") == 0,
                lit(None),
            ).otherwise(
                col("matched_silver_messages") / col("total_bronze_messages")
            ),
        )
        .withColumn(
            "valid_rate",
            when(
                col("matched_silver_messages") == 0,
                lit(None),
            ).otherwise(
                col("valid_events") / col("matched_silver_messages")
            ),
        )
        .withColumn(
            "invalid_rate",
            when(
                col("matched_silver_messages") == 0,
                lit(None),
            ).otherwise(
                col("invalid_events") / col("matched_silver_messages")
            ),
        )
        .withColumn(
            "late_arrival_rate",
            when(
                col("matched_silver_messages") == 0,
                lit(None),
            ).otherwise(
                col("late_arrival_events") / col("matched_silver_messages")
            ),
        )
    )

    return (
        metrics_df
        .select(
            "window_start",
            "window_end",
            "total_bronze_messages",
            "matched_silver_messages",
            "missing_in_silver_messages",

            spark_round("bronze_to_silver_match_rate", 4)
            .alias("bronze_to_silver_match_rate"),

            coalesce("valid_events", lit(0)).alias("valid_events"),
            coalesce("invalid_events", lit(0)).alias("invalid_events"),
            coalesce("late_arrival_events", lit(0)).alias("late_arrival_events"),

            spark_round("valid_rate", 4).alias("valid_rate"),
            spark_round("invalid_rate", 4).alias("invalid_rate"),
            spark_round("late_arrival_rate", 4).alias("late_arrival_rate"),

            spark_round("avg_latency_sec", 2).alias("avg_latency_sec"),
            spark_round("max_latency_sec", 2).alias("max_latency_sec"),

            current_timestamp().alias("updated_at"),
        )
    )


def upsert_quality_gold(spark: SparkSession, metrics_df):
    if not delta_table_exists(spark, GOLD_QUALITY_PATH):
        (
            metrics_df.write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_QUALITY_PATH)
        )
        logger.info(f"Successfully created Gold quality table at {GOLD_QUALITY_PATH}")
        return

    (
        DeltaTable.forPath(spark, GOLD_QUALITY_PATH)
        .alias("target")
        .merge(
            metrics_df.alias("source"),
            """
            target.window_start = source.window_start
            AND target.window_end = source.window_end
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Successfully upserted Gold quality metrics.")
