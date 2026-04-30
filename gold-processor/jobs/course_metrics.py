import logging
from config import SILVER_PATH, GOLD_COURSE_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def build_course_metrics(spark):
    logger.info("Start building course metrics.")

    spark.read.format("delta").load(SILVER_PATH).createOrReplaceTempView("silver")

    course_df = spark.sql("""
        WITH base AS (
            SELECT
                course_id,
                course_name,
                category,
                creator_id,
                creator_name,
                user_id,
                session_id,
                event_type,
                COALESCE(price, 0) AS price
            FROM silver
            WHERE event_quality_status IN ('valid', 'late_arrival')
        ),
        aggregated AS (
            SELECT
                course_id,
                course_name,
                category,
                creator_id,
                creator_name,

                COUNT(*)                  AS total_events,
                COUNT(DISTINCT user_id)   AS unique_users,
                COUNT(DISTINCT session_id) AS unique_sessions,

                SUM(CASE WHEN event_type = 'class_view'     THEN 1 ELSE 0 END) AS view_events,
                SUM(CASE WHEN event_type = 'video_play'     THEN 1 ELSE 0 END) AS play_events,
                SUM(CASE WHEN event_type = 'video_complete' THEN 1 ELSE 0 END) AS complete_events,
                SUM(CASE WHEN event_type = 'purchase'       THEN 1 ELSE 0 END) AS purchase_events,

                COUNT(DISTINCT CASE WHEN event_type = 'class_view'     THEN user_id END) AS view_users,
                COUNT(DISTINCT CASE WHEN event_type = 'video_play'     THEN user_id END) AS play_users,
                COUNT(DISTINCT CASE WHEN event_type = 'video_complete' THEN user_id END) AS complete_users,
                COUNT(DISTINCT CASE WHEN event_type = 'purchase'       THEN user_id END) AS purchase_users,

                SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS revenue,

                current_timestamp() AS updated_at
            FROM base
            GROUP BY
                course_id,
                course_name,
                category,
                creator_id,
                creator_name
        )
        SELECT
            *,
            CASE
                WHEN view_users > 0
                THEN CAST(purchase_users AS DOUBLE) / CAST(view_users AS DOUBLE)
                ELSE 0.0
            END AS view_to_purchase_rate
        FROM aggregated
    """)

    (
        course_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_COURSE_PATH)
    )

    logger.info("Finished building course metrics.")
