import os

# ClickHouse
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse123")
CH_DATABASE = os.getenv("CH_DATABASE", "otel")

# Enricher behavior
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "1"))      # seconds between polls
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "4096"))          # rows per embedding batch

# Embedding model
MODEL_NAME = os.getenv("MODEL_NAME", "all-MiniLM-L6-v2")
