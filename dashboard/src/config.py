import os

SILVER_PATH = os.getenv("SILVER_PATH", "s3://warehouse/silver/events")
GOLD_QUALITY_PATH = os.getenv("GOLD_QUALITY_PATH", "s3://warehouse/gold/data_quality_metrics")
GOLD_FUNNEL_PATH = os.getenv("GOLD_FUNNEL_PATH", "s3://warehouse/gold/funnel_metrics")
GOLD_COURSE_PATH = os.getenv("GOLD_COURSE_PATH", "s3://warehouse/gold/course_metrics")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
}