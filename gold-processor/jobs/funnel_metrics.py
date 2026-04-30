import logging
from config import SILVER_PATH, GOLD_FUNNEL_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def build_funnel_metrics(spark):
    logger.info("Start building funnel metrics.")

    spark.read.format("delta").load(SILVER_PATH).createOrReplaceTempView("silver")

    funnel_df = spark.sql("""
        WITH base AS (
            SELECT
                window(event_ts, '1 minute') AS w,
                user_segment,
                event_type,
                user_id,
                COALESCE(price, 0) AS price
            FROM silver
            WHERE event_quality_status IN ('valid', 'late_arrival')
        ),
        aggregated AS (
            SELECT
                w.start AS window_start,
                w.end   AS window_end,
                user_segment,

                COUNT(DISTINCT CASE WHEN event_type = 'class_view' THEN user_id END) AS view_users,
                COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchase_users,
                COUNT(DISTINCT CASE WHEN event_type = 'video_play' THEN user_id END) AS play_users,
                COUNT(DISTINCT CASE WHEN event_type = 'video_complete' THEN user_id END) AS complete_users,

                SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS revenue,

                current_timestamp() AS updated_at
            FROM base
            GROUP BY
                w,
                user_segment
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
        funnel_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_FUNNEL_PATH)
    )

    logger.info("Finished building funnel metrics.")
