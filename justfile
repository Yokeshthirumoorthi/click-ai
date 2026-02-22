compose := "docker compose -f docker-compose.dev.yaml"

# ─── Local dev (all zones) ────────────────────────────────────────

# Start all services locally
up:
    {{compose}} --profile clickhouse-local up -d --build

# Stop all services
down:
    {{compose}} --profile clickhouse-local --profile agent down -v

# ─── Production per-machine ───────────────────────────────────────

# Start data-plane (ClickHouse, MongoDB, HyperDX)
up-data:
    cd data-plane && docker compose up -d --build

# Stop data-plane
down-data:
    cd data-plane && docker compose down

# Start worker (s3-loader, embedding-enricher)
up-worker:
    cd worker && docker compose up -d --build

# Stop worker
down-worker:
    cd worker && docker compose down

# Start agent-plane
up-agent:
    cd agent-plane && docker compose up -d --build

# Stop agent-plane
down-agent:
    cd agent-plane && docker compose down

# ─── Logs ─────────────────────────────────────────────────────────

# Tail s3-loader logs
loader-logs:
    {{compose}} logs -f s3-loader

# ─── S3 Pipeline ──────────────────────────────────────────────────

# Show loader watermark status (processed / failed files) for all signal types
loader-status:
    docker exec clickhouse clickhouse-client --user admin --password clickhouse123 \
        --query "SELECT 'traces' AS signal, Status, count() AS cnt, sum(RowCount) AS total_rows FROM otel.loader_file_watermark FINAL GROUP BY Status UNION ALL SELECT 'logs', Status, count(), sum(RowCount) FROM otel.log_loader_file_watermark FINAL GROUP BY Status UNION ALL SELECT 'metrics', Status, count(), sum(RowCount) FROM otel.metric_loader_file_watermark FINAL GROUP BY Status ORDER BY signal"

# Quick row count check across all tables
bench:
    @echo "=== Table row counts ==="
    @docker exec clickhouse clickhouse-client --user admin --password clickhouse123 \
        --query "SELECT 'otel_traces' AS tbl, count() AS rows FROM otel.otel_traces UNION ALL SELECT 'otel_logs', count() FROM otel.otel_logs UNION ALL SELECT 'otel_metrics', count() FROM otel.otel_metrics UNION ALL SELECT 'loader_watermark', count() FROM otel.loader_file_watermark FINAL"

# ─── Analysis Platform ──────────────────────────────────────────

# Open the analysis platform in a browser
platform:
    @echo "Starting platform at http://localhost:3000"
    {{compose}} --profile clickhouse-local --profile agent up -d --build platform

# Tail platform logs
platform-logs:
    {{compose}} --profile clickhouse-local --profile agent logs -f platform

