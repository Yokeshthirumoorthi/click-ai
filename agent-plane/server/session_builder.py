"""Session builder — BACKUP TO S3 + download + ATTACH into chDB.

Flow: BACKUP table on master CH → S3 → download parts to chDB detached/ → ATTACH.
Works across separate machines — S3 is the only shared transport.
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

import boto3
from chdb import session as chdb_session
import clickhouse_connect

from . import config

log = logging.getLogger(__name__)

TABLES = ["otel_traces", "otel_logs", "otel_metrics"]

def _master_client():
    return clickhouse_connect.get_client(
        host=config.CH_HOST,
        port=config.CH_PORT,
        username=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DATABASE,
    )


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


# ── S3 backup path helpers ───────────────────────────────────────

def _backup_s3_path(session_id: str, table: str) -> str:
    """S3 URL for ClickHouse BACKUP/RESTORE S3() syntax."""
    return (
        f"{config.S3_ENDPOINT}/{config.S3_BACKUP_BUCKET}"
        f"/{session_id}/{table}/"
    )


def _backup_s3_prefix(session_id: str, table: str) -> str:
    """S3 key prefix for boto3 cleanup."""
    return f"{session_id}/{table}/"


# ── BACKUP on master → RESTORE in chDB ──────────────────────────

def _backup_table_to_s3(master, session_id: str, table: str, start: datetime, end: datetime) -> list[str]:
    """Run BACKUP on master CH, writing parts to S3. Returns partition IDs."""
    start_part = start.strftime("%Y%m%d")
    end_part = end.strftime("%Y%m%d")
    partitions = master.query(
        "SELECT DISTINCT partition_id FROM system.parts "
        f"WHERE database = 'otel' AND table = '{table}' AND active "
        f"AND partition_id >= '{start_part}' AND partition_id <= '{end_part}'",
    )
    partition_ids = [row[0] for row in partitions.result_rows]
    if not partition_ids:
        log.info("No partitions for %s in range %s–%s", table, start, end)
        return []

    partition_list = ", ".join(f"'{p}'" for p in partition_ids)
    s3_path = _backup_s3_path(session_id, table)

    backup_sql = (
        f"BACKUP TABLE otel.{table} "
        f"PARTITIONS {partition_list} "
        f"TO S3('{s3_path}', "
        f"'{config.S3_ACCESS_KEY}', '{config.S3_SECRET_KEY}')"
    )
    log.info("BACKUP %s (%d partitions) → S3", table, len(partition_ids))
    master.command(backup_sql)
    log.info("BACKUP %s complete", table)
    return partition_ids


def _restore_table_from_s3(session_id: str, table: str) -> int:
    """RESTORE a table from S3 backup into chDB session. Returns row count."""
    sess = _chdb_session(session_id)
    s3_path = _backup_s3_path(session_id, table)

    restore_sql = (
        f"RESTORE TABLE otel.{table} AS {table} "
        f"FROM S3('{s3_path}', "
        f"'{config.S3_ACCESS_KEY}', '{config.S3_SECRET_KEY}')"
    )
    log.info("RESTORE %s from S3 into chDB", table)
    sess.query(restore_sql)

    result = sess.query(f"SELECT count() FROM {table}", "JSON")
    data = json.loads(result.bytes())
    row_count = int(data["data"][0]["count()"])
    log.info("Table %s: %d rows in chDB session", table, row_count)
    return row_count


def _cleanup_s3_backup(s3, session_id: str, table: str):
    """Delete backup files from S3 after successful RESTORE."""
    prefix = _backup_s3_prefix(session_id, table)
    paginator = s3.get_paginator("list_objects_v2")

    objects = []
    for page in paginator.paginate(Bucket=config.S3_BACKUP_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({"Key": obj["Key"]})

    if objects:
        for i in range(0, len(objects), 1000):
            batch = objects[i : i + 1000]
            s3.delete_objects(
                Bucket=config.S3_BACKUP_BUCKET,
                Delete={"Objects": batch},
            )
        log.info("Cleaned up %d objects from S3 for %s/%s", len(objects), session_id, table)


def _backup_and_restore(
    master, s3, session_id: str, table: str, start: datetime, end: datetime,
) -> int:
    """Full flow: BACKUP on master → S3 → RESTORE in chDB."""
    partition_ids = _backup_table_to_s3(master, session_id, table, start, end)
    if not partition_ids:
        return 0

    try:
        return _restore_table_from_s3(session_id, table)
    finally:
        _cleanup_s3_backup(s3, session_id, table)


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
    """Build a session by BACKUP TO S3 → download → ATTACH into chDB."""
    master = _master_client()
    s3 = _s3_client()

    table_map = {"traces": "otel_traces", "logs": "otel_logs", "metrics": "otel_metrics"}
    tables_to_ship = [table_map[s] for s in signal_types if s in table_map]

    session_path = _session_dir(session_id)

    try:
        # 1. Create session dir
        log.info("Creating chDB session %s", session_id)
        session_path.mkdir(parents=True, exist_ok=True)

        # 2. BACKUP on master → S3 → RESTORE in chDB (creates tables automatically)
        counts = {}
        for table in tables_to_ship:
            signal = [s for s, t in table_map.items() if t == table][0]
            log.info("BACKUP+RESTORE %s ...", table)
            row_count = _backup_and_restore(master, s3, session_id, table, start, end)
            counts[signal] = row_count

        # 3. Build manifest
        manifest = _build_manifest(session_id)
        return {"counts": counts, "manifest": manifest}
    finally:
        master.close()


def drop_session(session_id: str):
    """Drop a session by removing its directory."""
    session_path = _session_dir(session_id)
    if session_path.exists():
        shutil.rmtree(session_path)
        log.info("Removed session directory %s", session_path)


def get_available_services() -> list[str]:
    ch = _master_client()
    try:
        result = ch.query(
            "SELECT DISTINCT ServiceName FROM otel_traces ORDER BY ServiceName"
        )
        return [row[0] for row in result.result_rows]
    finally:
        ch.close()
