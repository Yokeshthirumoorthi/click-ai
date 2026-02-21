import os

# S3 / MinIO
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "traces")
S3_PREFIX = os.getenv("S3_PREFIX", "incoming/")

# ClickHouse
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "9000"))
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse123")
CH_DATABASE = os.getenv("CH_DATABASE", "otel")

# Loader behavior
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))  # seconds
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))       # rows per INSERT
