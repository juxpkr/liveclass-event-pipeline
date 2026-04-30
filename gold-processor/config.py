import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://warehouse/bronze/events")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://warehouse/silver/events")
GOLD_QUALITY_PATH = os.getenv("GOLD_QUALITY_PATH", "s3a://warehouse/gold/data_quality_metrics")
GOLD_FUNNEL_PATH = os.getenv("GOLD_FUNNEL_PATH", "s3a://warehouse/gold/funnel_metrics")
GOLD_COURSE_PATH = os.getenv("GOLD_COURSE_PATH", "s3a://warehouse/gold/course_metrics")

GOLD_UPDATE_INTERVAL_SECONDS = int(os.getenv("GOLD_UPDATE_INTERVAL_SECONDS", "30"))
