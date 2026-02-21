import os

# S3 / MinIO
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "traces")
S3_TRACES_PREFIX = os.getenv("S3_TRACES_PREFIX", "incoming/")
S3_METRICS_PREFIX = os.getenv("S3_METRICS_PREFIX", "metrics/")
S3_LOGS_PREFIX = os.getenv("S3_LOGS_PREFIX", "logs/")

# ClickHouse
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "9000"))
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse123")
CH_DATABASE = os.getenv("CH_DATABASE", "otel")

# Loader behavior
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))  # seconds (legacy, used as fallback)
POLL_INTERVAL_BUSY = float(os.getenv("POLL_INTERVAL_BUSY", "0.5"))  # seconds between polls when files found
POLL_INTERVAL_IDLE = float(os.getenv("POLL_INTERVAL_IDLE", "2.0"))  # seconds between polls when idle
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50000"))       # rows per INSERT
MAX_FILE_WORKERS = int(os.getenv("MAX_FILE_WORKERS", "16"))  # concurrent S3 download threads per signal
