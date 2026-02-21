# SNKRS Load Generator

Direct ClickHouse bulk loader for generating realistic Nike SNKRS telemetry data at scale.

Bypasses the OTEL → S3 → loader pipeline for high-throughput bulk loading. Generates
correlated traces, logs, and metrics with realistic user behaviors, product drops,
and incident injection.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Make sure ClickHouse is running
just up

# Generate 10MB smoke test
just gen-xs

# Check results
just bench
```

## Volume Tiers

| Tier | Spans | Logs | Metrics | Size | Drops | Time Range | Est. Duration |
|------|-------|------|---------|------|-------|------------|---------------|
| xs | 10k | 5k | 2k | ~10 MB | 1 | 1h | ~5 sec |
| s | 100k | 50k | 20k | ~100 MB | 3 | 6h | ~30 sec |
| m | 1M | 500k | 200k | ~1 GB | 10 | 24h | ~5 min |
| l | 10M | 5M | 2M | ~10 GB | 50 | 7d | ~45 min |
| xl | 100M | 50M | 20M | ~100 GB | 200 | 30d | ~6-8 hrs |

## Architecture

- **users.py** — 10k Faker user profiles with behavior types (casual, hype, reseller, etc.)
- **inventory.py** — 24 products with per-(product, size) stock tracking
- **patterns.py** — Behavioral session templates + incident definitions
- **spans.py** — Distributed trace tree generator (8 microservices)
- **logs.py** — Correlated log records linked via TraceId/SpanId
- **metrics.py** — Time-series metrics at 10s intervals
- **writer.py** — Direct ClickHouse bulk inserter with multiprocessing
- **generate.py** — CLI orchestrator

## Options

```bash
python generate.py --tier m --workers 36 --ch-host localhost --ch-port 8123
```
