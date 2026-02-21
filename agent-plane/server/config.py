import os
from pathlib import Path

# ClickHouse
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse123")
CH_DATABASE = os.getenv("CH_DATABASE", "otel")

# OpenRouter LLM
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "anthropic/claude-sonnet-4")

# Auth
AUTH_SECRET = os.getenv("AUTH_SECRET", "change-me-in-production")
AUTH_USERNAME = os.getenv("AUTH_USERNAME", "admin")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD", "admin")
TOKEN_EXPIRY_HOURS = int(os.getenv("TOKEN_EXPIRY_HOURS", "24"))

# Sessions
SESSION_DIR = Path(os.getenv("SESSION_DIR", "/app/data/sessions"))
SESSION_DIR.mkdir(parents=True, exist_ok=True)

# Limits
MAX_ROWS_PER_TABLE = int(os.getenv("MAX_ROWS_PER_TABLE", "500000"))
