import os
from pathlib import Path

# Session data (local chDB storage)
SESSION_DIR = Path(os.getenv("SESSION_DIR", "/app/data/sessions"))

# S3 (backup source — master CH writes, droplets read)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BACKUP_BUCKET = os.getenv("S3_BACKUP_BUCKET", "ch-backups")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# OpenRouter LLM
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "anthropic/claude-sonnet-4")

# Auth
AUTH_SECRET = os.getenv("AUTH_SECRET", "change-me-in-production")
AUTH_USERNAME = os.getenv("AUTH_USERNAME", "admin")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD", "admin")
TOKEN_EXPIRY_HOURS = int(os.getenv("TOKEN_EXPIRY_HOURS", "24"))

# Limits
MAX_ROWS_PER_TABLE = int(os.getenv("MAX_ROWS_PER_TABLE", "500000"))
