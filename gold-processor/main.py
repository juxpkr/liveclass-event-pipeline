import logging
import time

from config import GOLD_UPDATE_INTERVAL_SECONDS
from spark_session import create_spark_session
from jobs.quality_metrics import build_data_quality_metrics, upsert_quality_gold
from jobs.funnel_metrics import build_funnel_metrics
from jobs.course_metrics import build_course_metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Gold Processor starting...")
    spark = create_spark_session()

    while True:
        try:
            quality_df = build_data_quality_metrics(spark)
            upsert_quality_gold(spark, quality_df)
        except Exception:
            logger.error("quality_metrics failed", exc_info=True)

        try:
            build_funnel_metrics(spark)
        except Exception:
            logger.error("funnel_metrics failed", exc_info=True)

        try:
            build_course_metrics(spark)
        except Exception:
            logger.error("course_metrics failed", exc_info=True)

        time.sleep(GOLD_UPDATE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
