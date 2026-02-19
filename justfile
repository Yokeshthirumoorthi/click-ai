# Start the infrastructure (ClickHouse, OTEL collector, Tabix, Grafana)
up:
    docker compose up -d

# Stop all services
down:
    docker compose down

# Generate test events (runs locally, sends to localhost:4317)
generate-events:
    cd event-generator && uv run send_test_events.py
