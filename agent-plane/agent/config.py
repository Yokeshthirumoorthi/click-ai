import os

# ClickHouse
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse123")
CH_DATABASE = os.getenv("CH_DATABASE", "otel")

# Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = os.getenv("AGENT_MODEL", "claude-sonnet-4-20250514")

# Embedding model (must match what the enricher uses)
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
