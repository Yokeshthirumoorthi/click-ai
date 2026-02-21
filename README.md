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
├── simulation/          # Dev only — mimics a customer
│   ├── docker-compose.yaml    # minio, otel-collector, event-generator
│   ├── event-generator/
│   └── config/                # OTEL collector config
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
| `simulation/` | minio, otel-collector, event-generator | Dev-only customer simulation |

## Prerequisites

- Docker & Docker Compose
- [just](https://github.com/casey/just) command runner
- `ANTHROPIC_API_KEY` env var (for the LLM agent only)

## Quick Start

```bash
just up      # start all services (all zones wired together)
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

| Command | What it does |
|---------|-------------|
| `just up` | Build & start all services locally |
| `just down` | Stop all services |
| `just up-data` | Start data-plane only (prod) |
| `just up-worker` | Start worker only (prod) |
| `just up-agent` | Start agent-plane only (prod) |
| `just logs` | Tail event generator logs |
| `just loader-logs` | Tail s3-loader logs |
| `just enricher-logs` | Tail embedding-enricher logs |
| `just loader-status` | Show processed/failed file counts |
| `just enrichment-status` | Show enriched row count + watermark |
| `just bench` | Row counts across all tables |
| `just minio-ls` | List files in the MinIO traces bucket |
| `just agent` | Run the LLM trace agent (interactive) |

## What's Running

| Container | What it does | URL / Port | Zone |
|-----------|-------------|------------|------|
| clickhouse | Column-store DB for all telemetry data | localhost:8123/9000 | data-plane |
| mongodb | HyperDX metadata storage | — | data-plane |
| hyperdx | HyperDX UI + API | localhost:8080 | data-plane |
| minio | S3-compatible storage (simulates customer) | localhost:9002/9001 | simulation |
| otel-collector | Receives, batches, exports to S3 | localhost:4317/4318 | simulation |
| event-generator | SNKRS drop simulator | — | simulation |
| s3-loader | Reads S3 → inserts into ClickHouse | — | worker |
| embedding-enricher | Computes vector embeddings for traces | — | worker |
| agent | LLM trace analysis CLI | — | agent-plane |

## The Drop Simulation

The event generator cycles through three phases, simulating a real SNKRS release:

| Phase | Duration | Workers | What happens |
|-------|----------|---------|-------------|
| PRE_DROP | 60s | 5 | Calm browsing, account checks, feed loads |
| DROP_LIVE | 90s | 40 | Draw entries, checkouts, inventory race |
| POST_DROP | 60s | 8 | Cooldown, order checks, notifications |

After each cycle the inventory resets and a new drop begins.

## Cleanup

```bash
just down                                                      # stop (keeps data)
docker compose -f docker-compose.dev.yaml down -v              # stop + delete all data
```
