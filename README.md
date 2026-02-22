# click-ai

Local observability playground — simulates Nike SNKRS drop traffic, pipes everything into ClickHouse, enriches traces with vector embeddings, and provides an LLM-powered trace analysis agent.

## Architecture

```
Event Generator
  → OTEL Collector
  → S3 (MinIO)
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

## Deployment Zones

The repo is organized by **deployment zone** — each top-level folder is a self-contained unit that maps to a cloud machine.

```
click-ai/
├── data-plane/          # ClickHouse machine (storage)
│   ├── docker-compose.yaml    # clickhouse, hyperdx, mongodb
│   └── config/                # ClickHouse server config + init schema
│
├── worker/              # GPU / processing machine
│   ├── docker-compose.yaml    # s3-loader, embedding-enricher
│   ├── s3-loader/
│   └── embedding-enricher/
│
├── agent-plane/         # Scalable agent machine(s)
│   ├── docker-compose.yaml    # agent (LLM trace query service)
│   ├── agent/
│   └── onboarding-api/        # future placeholder
│
├── docker-compose.dev.yaml    # Local dev overlay: wires all zones together
├── justfile
├── .env.example
└── docs/
    └── customer-otel-setup.md
```

| Zone | Services | Purpose |
|------|----------|---------|
| `data-plane/` | clickhouse, mongodb, hyperdx | Storage + observability UI |
| `worker/` | s3-loader, embedding-enricher | GPU workloads: S3 polling + embeddings |
| `agent-plane/` | agent | LLM trace analysis (scalable) |
| [snkrs-simulator](../snkrs-simulator) | minio, otel-collector, event-generator | Separate repo — traffic simulator |

## Prerequisites

- Docker & Docker Compose
- [just](https://github.com/casey/just) command runner
- `ANTHROPIC_API_KEY` env var (for the LLM agent only)

## Quick Start

```bash
just up      # start all services (all zones wired together)
```

Open http://localhost:8080 to see traces in HyperDX.

> **Note:** The traffic simulator (minio, otel-collector, event-generator) now lives in a separate repo: [snkrs-simulator](../snkrs-simulator). Start it first to feed telemetry into the pipeline.

### Verify the pipeline

```bash
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

| Command | What it does |
|---------|-------------|
| `just up` | Build & start all services locally |
| `just down` | Stop all services |
| `just up-data` | Start data-plane only (prod) |
| `just up-worker` | Start worker only (prod) |
| `just up-agent` | Start agent-plane only (prod) |
| `just loader-logs` | Tail s3-loader logs |
| `just enricher-logs` | Tail embedding-enricher logs |
| `just loader-status` | Show processed/failed file counts |
| `just enrichment-status` | Show enriched row count + watermark |
| `just bench` | Row counts across all tables |
| `just agent` | Run the LLM trace agent (interactive) |

## What's Running

| Container | What it does | URL / Port | Zone |
|-----------|-------------|------------|------|
| clickhouse | Column-store DB for all telemetry data | localhost:8123/9000 | data-plane |
| mongodb | HyperDX metadata storage | — | data-plane |
| hyperdx | HyperDX UI + API | localhost:8080 | data-plane |
| s3-loader | Reads S3 → inserts into ClickHouse | — | worker |
| embedding-enricher | Computes vector embeddings for traces | — | worker |
| agent | LLM trace analysis CLI | — | agent-plane |

## Cleanup

```bash
just down                                                      # stop (keeps data)
docker compose -f docker-compose.dev.yaml down -v              # stop + delete all data
```
