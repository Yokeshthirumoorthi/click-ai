# Start all services
up:
    docker compose up -d --build

# Stop all services
down:
    docker compose down

# Tail event generator logs
logs:
    docker compose logs -f event-generator

# Stop the event generator (HyperDX keeps running)
stop:
    docker compose stop event-generator

# Start the event generator
start:
    docker compose start event-generator

# Restart the event generator
restart:
    docker compose restart event-generator

# ─── S3 Pipeline ───────────────────────────────────────────────

# Tail s3-loader logs
loader-logs:
    docker compose logs -f s3-loader

# Tail embedding-enricher logs
enricher-logs:
    docker compose logs -f embedding-enricher

# Show loader watermark status (processed / failed files)
loader-status:
    docker exec clickhouse clickhouse-client --user admin --password clickhouse123 \
        --query "SELECT Status, count() AS cnt, sum(RowCount) AS total_rows FROM otel.loader_file_watermark FINAL GROUP BY Status"

# Show enrichment progress
enrichment-status:
    @echo "=== Enriched rows ==="
    @docker exec clickhouse clickhouse-client --user admin --password clickhouse123 \
        --query "SELECT count() AS enriched_rows FROM otel.otel_traces_enriched"
    @echo "=== Raw trace rows ==="
    @docker exec clickhouse clickhouse-client --user admin --password clickhouse123 \
        --query "SELECT count() AS raw_rows FROM otel.otel_traces"
    @echo "=== Enricher watermark ==="
    @docker exec clickhouse clickhouse-client --user admin --password clickhouse123 \
        --query "SELECT * FROM otel.enricher_watermark FINAL"

# Quick row count check across all tables
bench:
    @echo "=== Table row counts ==="
    @docker exec clickhouse clickhouse-client --user admin --password clickhouse123 \
        --query "SELECT 'otel_traces' AS tbl, count() AS rows FROM otel.otel_traces UNION ALL SELECT 'otel_traces_enriched', count() FROM otel.otel_traces_enriched UNION ALL SELECT 'loader_watermark', count() FROM otel.loader_file_watermark FINAL"

# Run the LLM trace agent (interactive)
agent:
    docker compose --profile agent run --rm agent

# List files in the MinIO traces bucket
minio-ls:
    docker run --rm --network=click-ai_default --entrypoint /bin/sh minio/mc -c \
        "mc alias set local http://minio:9000 minioadmin minioadmin > /dev/null 2>&1 && mc ls --recursive local/traces/incoming/"
