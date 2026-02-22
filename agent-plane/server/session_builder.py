"""Session builder — S3-only restore into chDB.

Flow: Read partition inventory from S3 → RESTORE partitions into chDB.
Droplets never connect to master CH — S3 is the only data source.
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

import boto3
from chdb import session as chdb_session

from . import config

log = logging.getLogger(__name__)

TABLES = ["otel_traces", "otel_logs", "otel_metrics"]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name=config.S3_REGION,
    )


def _session_dir(session_id: str) -> Path:
    return config.SESSION_DIR / session_id


def _chdb_session(session_id: str) -> chdb_session.Session:
    path = str(_session_dir(session_id))
    return chdb_session.Session(path)


# ── S3 partition discovery ───────────────────────────────────────

def _get_available_partitions(s3, table: str) -> list[str]:
    """List available partition IDs for a table on S3 using delimiter listing."""
    prefix = f"{table}/"
    partitions = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=config.S3_BACKUP_BUCKET,
        Prefix=prefix,
        Delimiter="/",
    ):
        for cp in page.get("CommonPrefixes", []):
            # cp["Prefix"] looks like "otel_traces/20260221/"
            part_id = cp["Prefix"].rstrip("/").split("/")[-1]
            partitions.append(part_id)

    return sorted(partitions)


# ── S3 → chDB restore ───────────────────────────────────────────

def _restore_partitions_from_s3(
    session_id: str, table: str, partition_ids: list[str],
) -> int:
    """RESTORE partitions from S3 backup into chDB session. Returns row count."""
    sess = _chdb_session(session_id)
    restored = 0

    for part_id in partition_ids:
        s3_path = (
            f"{config.S3_ENDPOINT}/{config.S3_BACKUP_BUCKET}"
            f"/{table}/{part_id}/"
        )
        restore_sql = (
            f"RESTORE TABLE otel.{table} AS {table} "
            f"FROM S3('{s3_path}', "
            f"'{config.S3_ACCESS_KEY}', '{config.S3_SECRET_KEY}')"
        )
        log.info("RESTORE %s partition %s from S3", table, part_id)
        sess.query(restore_sql)
        restored += 1

    if restored == 0:
        return 0

    result = sess.query(f"SELECT count() FROM {table}", "JSON")
    data = json.loads(result.bytes())
    row_count = int(data["data"][0]["count()"])
    log.info("Table %s: %d rows in chDB session (%d partitions)", table, row_count, restored)
    return row_count


# ── Manifest ─────────────────────────────────────────────────────

def _build_manifest(session_id: str) -> dict:
    """Build manifest from chDB session tables."""
    sess = _chdb_session(session_id)
    tables = {}
    for table_name in TABLES:
        try:
            count_result = sess.query(f"SELECT count() FROM {table_name}", "JSON")
            count_data = json.loads(count_result.bytes())
            count = int(count_data["data"][0]["count()"])
            if count == 0:
                continue

            cols_result = sess.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = 'default' AND table = '{table_name}' "
                f"ORDER BY position",
                "JSON",
            )
            cols_data = json.loads(cols_result.bytes())

            sample_result = sess.query(f"SELECT * FROM {table_name} LIMIT 3", "JSON")
            sample_data = json.loads(sample_result.bytes())
            sample_rows = []
            for row in sample_data.get("data", [])[:3]:
                sample_rows.append({k: str(v) for k, v in row.items()})

            tables[table_name] = {
                "row_count": count,
                "columns": [{"name": r["name"], "type": r["type"]} for r in cols_data["data"]],
                "sample_rows": sample_rows,
            }
        except Exception as e:
            log.warning("Manifest for %s: %s", table_name, e)
            continue
    return tables


# ── Public API ───────────────────────────────────────────────────

def build_session(
    session_id: str,
    services: list[str],
    signal_types: list[str],
    start: datetime,
    end: datetime,
) -> dict:
    """Build a session by restoring partitions from S3 into chDB."""
    s3 = _s3_client()

    table_map = {"traces": "otel_traces", "logs": "otel_logs", "metrics": "otel_metrics"}
    tables_to_ship = [table_map[s] for s in signal_types if s in table_map]

    session_path = _session_dir(session_id)

    # Compute date range partition IDs
    start_part = start.strftime("%Y%m%d")
    end_part = end.strftime("%Y%m%d")

    # 1. Create session dir
    log.info("Creating chDB session %s", session_id)
    session_path.mkdir(parents=True, exist_ok=True)

    # 2. For each table: list S3 partitions, filter to date range, RESTORE
    counts = {}
    for table in tables_to_ship:
        signal = [s for s, t in table_map.items() if t == table][0]

        available = _get_available_partitions(s3, table)
        in_range = [p for p in available if start_part <= p <= end_part]

        if not in_range:
            log.info("No S3 partitions for %s in range %s–%s", table, start_part, end_part)
            counts[signal] = 0
            continue

        log.info("Restoring %s: %d partitions from S3", table, len(in_range))
        row_count = _restore_partitions_from_s3(session_id, table, in_range)
        counts[signal] = row_count

    # 3. Build manifest
    manifest = _build_manifest(session_id)
    return {"counts": counts, "manifest": manifest}


def drop_session(session_id: str):
    """Drop a session by removing its directory."""
    session_path = _session_dir(session_id)
    if session_path.exists():
        shutil.rmtree(session_path)
        log.info("Removed session directory %s", session_path)


def get_available_services() -> list[str]:
    """Read service list from metadata.json on S3."""
    s3 = _s3_client()
    try:
        response = s3.get_object(
            Bucket=config.S3_BACKUP_BUCKET,
            Key="metadata.json",
        )
        metadata = json.loads(response["Body"].read())
        return sorted(metadata.get("services", []))
    except s3.exceptions.NoSuchKey:
        log.warning("metadata.json not found on S3 — no backups yet?")
        return []
    except Exception as e:
        log.warning("Failed to read metadata.json from S3: %s", e)
        return []
