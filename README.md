# click-ai

## Prerequisites

- Docker & Docker Compose
- [just](https://github.com/casey/just) command runner
- [uv](https://github.com/astral-sh/uv) Python package manager

## Folder Structure

```
click-ai/
├── justfile
├── docker-compose.yaml
├── config/
│   ├── clickhouse-config.xml
│   ├── clickhouse-users.xml
│   └── otel-collector-config.yaml
├── event-generator/
│   ├── pyproject.toml
│   └── send_test_events.py
└── otel_queries.sql
```

## Quick Start

```bash
# Start infrastructure (ClickHouse, OTEL collector, Tabix, Grafana)
just up

# In another terminal, start generating fake events
just generate-events
```

## Just Commands

| Command                | What it does                                          |
| ---------------------- | ----------------------------------------------------- |
| `just up`              | Start Docker infrastructure                           |
| `just down`            | Stop all Docker services                              |
| `just generate-events` | Run event generator locally (sends to localhost:4317) |

## What's Running

| Container      | What it does                          | URL / Port            |
| -------------- | ------------------------------------- | --------------------- |
| clickhouse     | The database                          | localhost:8123 / 9000 |
| otel-collector | Receives events, writes to ClickHouse | localhost:4317 (gRPC) |
| tabix          | SQL browser UI                        | http://localhost:8080  |
| grafana        | Dashboards                            | http://localhost:3000  |

## Query Your Data in Tabix

1. Open http://localhost:8080
2. Host: http://localhost:8123 | Login: admin | Password: clickhouse123
3. Paste queries from otel_queries.sql

## Connect Your Real App

Point your OTEL SDK at:

- gRPC: localhost:4317
- HTTP: localhost:4318

## Cleanup

```bash
just down                  # stop (keeps data)
docker compose down -v     # stop + delete all data
```
