# click-ai

Local observability playground — simulates Nike SNKRS drop traffic, pipes everything into ClickHouse, enriches traces with vector embeddings, and provides an LLM-powered trace analysis agent.

## Architecture

```
Event Generator
  → OTEL Collector
  → S3 (MinIO) + ClickHouse (direct)
         │
         │ read-only
         ▼
    S3 Loader ──────────► ClickHouse (otel.otel_traces)
                                   │
                                   ▼
                          Embedding Enricher ──► otel.otel_traces_enriched
                                                        │
                                                        ▼
                                                   LLM Agent
                                              (Claude + tool use)
```

**4 isolated stages:** event generation → S3 loading → embedding enrichment → LLM analysis. Each is a separate process; if one fails, the others are unaffected.

## Prerequisites

- Docker & Docker Compose
- [just](https://github.com/casey/just) command runner
- `ANTHROPIC_API_KEY` env var (for the LLM agent only)

## Folder Structure

```
click-ai/
├── justfile
├── docker-compose.yaml
├── otel_queries.sql
├── config/
│   ├── clickhouse-config.xml
│   ├── clickhouse-users.xml
│   ├── init-db.sql
│   ├── nginx-otel.conf
│   └── otel-collector-config.yaml
├── event-generator/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── send_test_events.py
│   └── snkrs_queries.sql
├── s3-loader/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── config.py
│   └── loader.py
├── embedding-enricher/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── config.py
│   └── enricher.py
├── agent/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── config.py
│   ├── agent.py
│   ├── tools.py
│   └── prompts.py
└── README.md
```

## Quick Start

```bash
just up      # start all services (collector, loader, enricher, etc.)
just logs    # tail the simulator output
```

Open http://localhost:8080 to see traces in HyperDX.

### Verify the pipeline

```bash
just minio-ls            # OTLP JSON files appearing in S3
just loader-status       # files being processed by the loader
just bench               # row counts in ClickHouse
just enrichment-status   # embeddings being computed
```

### Run the LLM agent

```bash
export ANTHROPIC_API_KEY=sk-ant-...
just agent    # interactive trace analysis CLI
```

Ask questions like:
- "find slow checkout spans in the last hour"
- "what services have the most errors?"
- "show me the trace tree for a failed payment"

## Just Commands

| Command                  | What it does                                     |
| ------------------------ | ------------------------------------------------ |
| `just up`                | Build & start all services                       |
| `just down`              | Stop all services                                |
| `just logs`              | Tail event generator logs                        |
| `just stop`              | Stop event generator (HyperDX keeps running)     |
| `just start`             | Start event generator                            |
| `just restart`           | Restart event generator                          |
| `just loader-logs`       | Tail s3-loader logs                              |
| `just enricher-logs`     | Tail embedding-enricher logs                     |
| `just loader-status`     | Show processed/failed file counts                |
| `just enrichment-status` | Show enriched row count + watermark              |
| `just bench`             | Row counts across all tables                     |
| `just minio-ls`          | List files in the MinIO traces bucket            |
| `just agent`             | Run the LLM trace agent (interactive)            |

## What's Running

| Container           | What it does                                    | URL / Port           | Always on? |
| ------------------- | ----------------------------------------------- | -------------------- | ---------- |
| clickhouse          | Column-store DB for all telemetry data          | localhost:8123/9000  | Yes        |
| otel-collector      | Receives, batches, exports to CH + S3           | localhost:4317/4318  | Yes        |
| minio               | S3-compatible storage (simulates customer)      | localhost:9002/9001  | Yes        |
| mongodb             | HyperDX metadata storage                        | —                    | Yes        |
| hyperdx             | HyperDX UI + API                                | localhost:8080       | Yes        |
| event-generator     | SNKRS drop simulator                            | —                    | Yes        |
| s3-loader           | Reads S3 → inserts into ClickHouse              | —                    | Yes        |
| embedding-enricher  | Computes vector embeddings for traces           | —                    | Yes        |
| agent               | LLM trace analysis CLI                          | —                    | On-demand  |

## The Drop Simulation

The event generator cycles through three phases, simulating a real SNKRS release:

| Phase       | Duration | Workers | What happens                                  |
| ----------- | -------- | ------- | --------------------------------------------- |
| PRE_DROP    | 60s      | 5       | Calm browsing, account checks, feed loads     |
| DROP_LIVE   | 90s      | 40      | Draw entries, checkouts, inventory race        |
| POST_DROP   | 60s      | 8       | Cooldown, order checks, notifications          |

After each cycle the inventory resets and a new drop begins.

## Raw SQL Queries

Connect to ClickHouse at `localhost:8123` and run queries from `otel_queries.sql` or `event-generator/snkrs_queries.sql`.

## Cleanup

```bash
just down                  # stop (keeps data)
docker compose down -v     # stop + delete all data
```
