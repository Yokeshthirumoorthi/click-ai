#!/usr/bin/env python3
"""
Nike SNKRS Load Generator — OTLP pipeline loader.

Generates realistic telemetry (traces, logs, metrics) and sends them
through the OTEL Collector via gRPC, following the production pipeline:
  OTLP gRPC → Collector → S3 → s3-loader → ClickHouse → Enricher

Usage:
    python generate.py --tier xs    # 10 MB   (~10k spans)
    python generate.py --tier s     # 100 MB  (~100k spans)
    python generate.py --tier m     # 1 GB    (~1M spans)
    python generate.py --tier l     # 10 GB   (~10M spans)
    python generate.py --tier xl    # 100 GB  (~100M spans)
"""

import argparse
import math
import multiprocessing
import os
import random
import sys
import time
from datetime import datetime, timedelta

from users import get_users
from inventory import select_drop_product, StockTracker, PRODUCTS
from patterns import get_incident_for_drop, PATTERNS
from metrics import generate_metrics_for_interval
from writer import OTLPWriter, worker_fn

# ── Tier definitions ────────────────────────────────────────────────────

TIERS = {
    "xs": {
        "label": "XS — Smoke Test",
        "target_spans": 10_000,
        "drops": 1,
        "time_range_hours": 1,
        "users_per_drop": 100,
        "sessions_multiplier": 1.0,
    },
    "s": {
        "label": "S — Basic Validation",
        "target_spans": 100_000,
        "drops": 3,
        "time_range_hours": 6,
        "users_per_drop": 500,
        "sessions_multiplier": 1.0,
    },
    "m": {
        "label": "M — Medium Load",
        "target_spans": 1_000_000,
        "drops": 10,
        "time_range_hours": 24,
        "users_per_drop": 2_000,
        "sessions_multiplier": 1.2,
    },
    "l": {
        "label": "L — Large Scale",
        "target_spans": 10_000_000,
        "drops": 50,
        "time_range_hours": 168,  # 7 days
        "users_per_drop": 5_000,
        "sessions_multiplier": 1.5,
    },
    "xl": {
        "label": "XL — Full Demo",
        "target_spans": 100_000_000,
        "drops": 200,
        "time_range_hours": 720,  # 30 days
        "users_per_drop": 10_000,
        "sessions_multiplier": 2.0,
    },
}


def build_drop_schedule(tier: dict, start_time: datetime) -> list[tuple]:
    """Build a list of (drop_index, product, incident, drop_start, drop_end) tuples.

    Drops are spaced evenly across the time range with some jitter.
    """
    n_drops = tier["drops"]
    total_hours = tier["time_range_hours"]
    interval_hours = total_hours / max(1, n_drops)
    rng = random.Random(12345)

    schedule = []
    for i in range(n_drops):
        # Space drops evenly with jitter
        jitter_hours = rng.uniform(-interval_hours * 0.1, interval_hours * 0.1)
        drop_start = start_time + timedelta(hours=i * interval_hours + jitter_hours)
        # Each drop lasts 15-45 min depending on tier
        drop_duration_min = rng.uniform(15, 45)
        drop_end = drop_start + timedelta(minutes=drop_duration_min)

        product = select_drop_product(i, rng)
        incident = get_incident_for_drop(i)

        schedule.append((i, product, incident, drop_start, drop_end))

    return schedule


def partition_users(users: list[dict], n_workers: int) -> list[list[dict]]:
    """Split users into roughly equal chunks for workers."""
    chunk_size = math.ceil(len(users) / n_workers)
    return [users[i:i + chunk_size] for i in range(0, len(users), chunk_size)]


def generate_metrics_centrally(drop_schedule: list, tier: dict,
                               otel_config: dict):
    """Generate metrics in the main process (not parallelized — they're small)."""
    print("\n  Generating metrics...")
    writer = OTLPWriter(
        endpoint=otel_config["endpoint"],
        batch_size=otel_config.get("batch_size", 5_000),
    )
    writer.connect()

    rng = random.Random(54321)

    for drop_idx, product, incident, drop_start, drop_end in drop_schedule:
        drop_duration = (drop_end - drop_start).total_seconds()
        stock_tracker = StockTracker()
        stock_tracker.reset_for_drop(product, rng)
        total_stock = stock_tracker.get_total_stock(product["id"])
        queue_depth = 0

        # Generate metrics at 10s intervals across the drop
        ts = drop_start - timedelta(minutes=5)  # start 5 min before drop
        end_ts = drop_end + timedelta(minutes=5)  # end 5 min after

        while ts < end_ts:
            progress = max(0, min(1, (ts - drop_start).total_seconds() / max(1, drop_duration)))

            if ts < drop_start:
                phase = "pre_drop"
                active_users = int(tier["users_per_drop"] * 0.1 * rng.uniform(0.5, 1.5))
            elif ts < drop_end:
                phase = "drop_live"
                active_users = int(tier["users_per_drop"] * rng.uniform(0.6, 1.0))
                # Stock decreases during drop
                sell_rate = 0.02 * rng.uniform(0.5, 2.0)
                total_stock = max(0, int(total_stock * (1 - sell_rate)))
                queue_depth = int(active_users * 0.3 * rng.uniform(0.5, 1.5))
            else:
                phase = "post_drop"
                active_users = int(tier["users_per_drop"] * 0.15 * rng.uniform(0.3, 1.0))
                queue_depth = max(0, queue_depth - rng.randint(10, 50))

            metrics = generate_metrics_for_interval(
                ts, phase, product, total_stock, queue_depth,
                active_users, incident=incident, rng=rng)
            writer.add_metrics(metrics)

            ts += timedelta(seconds=10)

    writer.flush_all()
    stats = writer.stats()
    writer.close()
    return stats


def run(tier_name: str, workers: int, otel_endpoint: str, batch_size: int):
    """Main generation orchestrator."""
    tier = TIERS[tier_name]
    print("=" * 65)
    print(f"  Nike SNKRS Load Generator — {tier['label']}")
    print("=" * 65)
    print(f"  Target:     ~{tier['target_spans']:,} spans")
    print(f"  Drops:      {tier['drops']}")
    print(f"  Time range: {tier['time_range_hours']}h")
    print(f"  Users/drop: {tier['users_per_drop']:,}")
    print(f"  Workers:    {workers}")
    print(f"  Batch size: {batch_size:,}")
    print(f"  OTEL endpoint: {otel_endpoint}")
    print("=" * 65)

    start_time = time.time()

    # Generate user population
    print(f"\n[1/4] Generating {tier['users_per_drop']:,} user profiles...")
    users = get_users(tier["users_per_drop"])
    print(f"  Done — {len(users):,} users generated")

    # Build drop schedule
    print(f"\n[2/4] Building drop schedule ({tier['drops']} drops over {tier['time_range_hours']}h)...")
    # End time is "now" so data looks recent
    end_time = datetime.utcnow()
    schedule_start = end_time - timedelta(hours=tier["time_range_hours"])
    drop_schedule = build_drop_schedule(tier, schedule_start)

    for i, (idx, prod, inc, ds, de) in enumerate(drop_schedule):
        inc_label = f" [INCIDENT: {inc['name']}]" if inc else ""
        print(f"  Drop {i+1}: {prod['name'][:40]} | {ds.strftime('%Y-%m-%d %H:%M')} → {de.strftime('%H:%M')}{inc_label}")

    # Generate spans + logs via multiprocessing
    print(f"\n[3/4] Generating spans + logs with {workers} workers...")

    otel_config = {
        "endpoint": otel_endpoint,
        "batch_size": batch_size,
    }

    # Partition users across workers
    user_chunks = partition_users(users, workers)
    # Adjust: if we have more workers than users, trim
    actual_workers = len(user_chunks)

    # Build worker args
    worker_args = [
        (chunk, (schedule_start, end_time), drop_schedule, otel_config)
        for chunk in user_chunks
    ]

    print(f"  Launching {actual_workers} worker processes...")
    pool_start = time.time()

    with multiprocessing.Pool(processes=actual_workers) as pool:
        results = pool.map(worker_fn, worker_args)

    pool_elapsed = time.time() - pool_start

    # Aggregate stats
    total_spans = sum(r["spans"] for r in results)
    total_logs = sum(r["logs"] for r in results)
    total_rows = sum(r["total"] for r in results)

    print(f"\n  Spans + Logs complete in {pool_elapsed:.1f}s")
    print(f"  Spans:  {total_spans:,}")
    print(f"  Logs:   {total_logs:,}")
    print(f"  Rate:   {total_rows / max(1, pool_elapsed):,.0f} rows/sec")

    # Generate metrics (small, single process)
    print(f"\n[4/4] Generating metrics...")
    metric_stats = generate_metrics_centrally(drop_schedule, tier, otel_config)
    print(f"  Metrics: {metric_stats['metrics']:,}")

    # Final summary
    elapsed = time.time() - start_time
    grand_total = total_spans + total_logs + metric_stats["metrics"]
    print("\n" + "=" * 65)
    print(f"  COMPLETE — {tier['label']}")
    print(f"  Spans:   {total_spans:,}")
    print(f"  Logs:    {total_logs:,}")
    print(f"  Metrics: {metric_stats['metrics']:,}")
    print(f"  Total:   {grand_total:,} rows")
    print(f"  Time:    {elapsed:.1f}s")
    print(f"  Rate:    {grand_total / max(1, elapsed):,.0f} rows/sec")
    print("=" * 65)


def main():
    parser = argparse.ArgumentParser(
        description="Nike SNKRS Load Generator — OTLP pipeline loader")
    parser.add_argument("--tier", choices=["xs", "s", "m", "l", "xl"],
                        default="xs", help="Volume tier (default: xs)")
    parser.add_argument("--workers", type=int, default=None,
                        help="Number of worker processes (default: cpu_count - 4)")
    parser.add_argument("--otel-endpoint",
                        default=os.getenv("OTEL_ENDPOINT", "localhost:4317"),
                        help="OTEL Collector gRPC endpoint (default: localhost:4317)")
    parser.add_argument("--batch-size", type=int, default=5_000,
                        help="Events per gRPC batch (default: 5000)")

    args = parser.parse_args()

    if args.workers is None:
        cpu_count = multiprocessing.cpu_count()
        args.workers = max(1, min(cpu_count - 4, 36))

    run(args.tier, args.workers, args.otel_endpoint, args.batch_size)


if __name__ == "__main__":
    main()
