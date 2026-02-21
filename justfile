compose := "docker compose -f docker-compose.dev.yaml"

# ─── Local dev (all zones) ────────────────────────────────────────

# Start all services locally
up:
    {{compose}} --profile clickhouse-local up -d --build

# Stop all services
down:
    {{compose}} --profile clickhouse-local down -v

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

# ─── Simulation ─────────────────────────────────────────────────────

# Start the event generator
start-sim:
    {{compose}} --profile clickhouse-local start event-generator

# Stop the event generator
stop-sim:
    {{compose}} --profile clickhouse-local stop event-generator

# ─── Logs ─────────────────────────────────────────────────────────

# Tail event generator logs
logs:
    {{compose}} logs -f event-generator

# Tail s3-loader logs
loader-logs:
    {{compose}} logs -f s3-loader

# Tail embedding-enricher logs
enricher-logs:
    {{compose}} logs -f embedding-enricher

# ─── S3 Pipeline ──────────────────────────────────────────────────

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

# ─── Agent ────────────────────────────────────────────────────────

# Run the LLM trace agent (interactive)
agent:
    {{compose}} --profile agent run --rm agent

# ─── Utilities ────────────────────────────────────────────────────

# List files in the MinIO traces bucket
minio-ls:
    docker run --rm --network=click-ai_default --entrypoint /bin/sh minio/mc -c \
        "mc alias set local http://minio:9000 minioadmin minioadmin > /dev/null 2>&1 && mc ls --recursive local/traces/incoming/"
