# click-ai

Local observability playground — simulates Nike SNKRS drop traffic and pipes everything (traces, logs, metrics) into [HyperDX](https://www.hyperdx.io/).

## Prerequisites

- Docker & Docker Compose
- [just](https://github.com/casey/just) command runner

## Folder Structure

```
click-ai/
├── justfile
├── docker-compose.yaml
├── otel_queries.sql
├── config/
│   ├── clickhouse-config.xml
│   ├── clickhouse-users.xml
│   ├── nginx-otel.conf
│   └── otel-collector-config.yaml
├── event-generator/
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── send_test_events.py
│   └── snkrs_queries.sql
└── README.md
```

## Quick Start

```bash
just up      # start all services
just logs    # tail the simulator output
```

Open http://localhost:8080 to see traces, logs, and metrics in HyperDX.

## Just Commands

| Command        | What it does                                     |
| -------------- | ------------------------------------------------ |
| `just up`      | Build & start all services                       |
| `just down`    | Stop all services                                |
| `just logs`    | Tail event generator logs                        |
| `just stop`    | Stop event generator (HyperDX keeps running)     |
| `just start`   | Start event generator                            |
| `just restart` | Restart event generator                          |
| `just bench`   | Show per-second ingestion throughput (last 60s)  |

## What's Running

| Container        | What it does                                    | URL / Port           |
| ---------------- | ----------------------------------------------- | -------------------- |
| clickhouse       | Column-store DB for all telemetry data          | localhost:8123/9000  |
| otel-collector   | Receives, batches, exports to ClickHouse        | localhost:4317/4318  |
| mongodb          | HyperDX metadata storage                       | —                    |
| hyperdx          | HyperDX UI + API                                | http://localhost:8080 |
| event-generator  | SNKRS drop simulator (traces + logs + metrics)  | —                    |

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
