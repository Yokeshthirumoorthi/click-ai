"""Session builder — ships parts from master ClickHouse, queries via chDB (embedded).

Flow: FREEZE partitions on master → copy parts to local session dir → ATTACH via chDB.
Each session gets its own directory for chDB isolation.
"""

import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import chdb
import clickhouse_connect

from . import config

log = logging.getLogger(__name__)

# Mount point for master CH data inside the platform container
MASTER_CH_DATA = Path(os.getenv("MASTER_CH_DATA", "/master-ch-data"))

TABLES = ["otel_traces", "otel_logs", "otel_metrics"]

# Table DDL for session databases — must match master's partition/order keys exactly.
_TABLE_DDL = {
    "otel_traces": """
        CREATE TABLE IF NOT EXISTS otel_traces (
            Timestamp          DateTime64(9)                           CODEC(Delta, ZSTD(1)),
            TraceId            String                                  CODEC(ZSTD(1)),
            SpanId             String                                  CODEC(ZSTD(1)),
            ParentSpanId       String                                  CODEC(ZSTD(1)),
            TraceState         String                                  CODEC(ZSTD(1)),
            SpanName           LowCardinality(String)                  CODEC(ZSTD(1)),
            SpanKind           LowCardinality(String)                  CODEC(ZSTD(1)),
            ServiceName        LowCardinality(String)                  CODEC(ZSTD(1)),
            ResourceAttributes Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
            ScopeName          String                                  CODEC(ZSTD(1)),
            ScopeVersion       String                                  CODEC(ZSTD(1)),
            SpanAttributes     Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
            Duration           UInt64                                  CODEC(ZSTD(1)),
            StatusCode         LowCardinality(String)                  CODEC(ZSTD(1)),
            StatusMessage      String                                  CODEC(ZSTD(1)),
            `Events.Timestamp`   Array(DateTime64(9))                  CODEC(ZSTD(1)),
            `Events.Name`        Array(LowCardinality(String))         CODEC(ZSTD(1)),
            `Events.Attributes`  Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
            `Links.TraceId`      Array(String)                         CODEC(ZSTD(1)),
            `Links.SpanId`       Array(String)                         CODEC(ZSTD(1)),
            `Links.TraceState`   Array(String)                         CODEC(ZSTD(1)),
            `Links.Attributes`   Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1))
        ) ENGINE = MergeTree()
        PARTITION BY toDate(Timestamp)
        ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
        SETTINGS index_granularity = 8192
    """,
    "otel_logs": """
        CREATE TABLE IF NOT EXISTS otel_logs (
            Timestamp          DateTime64(9)                           CODEC(Delta, ZSTD(1)),
            TraceId            String                                  CODEC(ZSTD(1)),
            SpanId             String                                  CODEC(ZSTD(1)),
            SeverityNumber     UInt8                                   CODEC(ZSTD(1)),
            SeverityText       LowCardinality(String)                  CODEC(ZSTD(1)),
            Body               String                                  CODEC(ZSTD(1)),
            ServiceName        LowCardinality(String)                  CODEC(ZSTD(1)),
            ResourceAttributes Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
            LogAttributes      Map(LowCardinality(String), String)     CODEC(ZSTD(1))
        ) ENGINE = MergeTree()
        PARTITION BY toDate(Timestamp)
        ORDER BY (ServiceName, SeverityText, toDateTime(Timestamp))
        SETTINGS index_granularity = 8192
    """,
    "otel_metrics": """
        CREATE TABLE IF NOT EXISTS otel_metrics (
            Timestamp          DateTime64(9)                           CODEC(Delta, ZSTD(1)),
            MetricName         LowCardinality(String)                  CODEC(ZSTD(1)),
            MetricDescription  String                                  CODEC(ZSTD(1)),
            MetricUnit         String                                  CODEC(ZSTD(1)),
            MetricType         LowCardinality(String)                  CODEC(ZSTD(1)),
            Value              Float64                                 CODEC(ZSTD(1)),
            ServiceName        LowCardinality(String)                  CODEC(ZSTD(1)),
            ResourceAttributes Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
            MetricAttributes   Map(LowCardinality(String), String)     CODEC(ZSTD(1))
        ) ENGINE = MergeTree()
        PARTITION BY toDate(Timestamp)
        ORDER BY (ServiceName, MetricName, toDateTime(Timestamp))
        SETTINGS index_granularity = 8192
    """,
}


def _master_client():
    return clickhouse_connect.get_client(
        host=config.CH_HOST,
        port=config.CH_PORT,
        username=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DATABASE,
    )


def _session_dir(session_id: str) -> Path:
    return config.SESSION_DIR / session_id


def _chdb_session(session_id: str) -> chdb.Session:
    path = str(_session_dir(session_id))
    return chdb.Session(path)


def _date_range(start: datetime, end: datetime) -> list[str]:
    """Return YYYYMMDD partition IDs covering the time range."""
    d = start.date()
    end_d = end.date()
    dates = []
    while d <= end_d:
        dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return dates


def _date_values(start: datetime, end: datetime) -> list[str]:
    """Return YYYY-MM-DD date strings for FREEZE PARTITION."""
    d = start.date()
    end_d = end.date()
    dates = []
    while d <= end_d:
        dates.append(d.isoformat())
        d += timedelta(days=1)
    return dates


# ── Schema setup (chDB) ─────────────────────────────────────────

def _create_session_tables(session_id: str, signal_types: list[str]):
    """Create tables in a chDB session."""
    sess = _chdb_session(session_id)
    table_map = {"traces": "otel_traces", "logs": "otel_logs", "metrics": "otel_metrics"}
    for signal in signal_types:
        table = table_map.get(signal)
        if table and table in _TABLE_DDL:
            sess.query(_TABLE_DDL[table])


# ── FREEZE on master ─────────────────────────────────────────────

def _freeze_partitions(ch, session_id: str, table: str, date_values: list[str]):
    """Freeze date partitions on master CH. Creates hard-links in shadow/."""
    backup_name = f"session_{session_id}"
    for dt in date_values:
        try:
            ch.command(
                f"ALTER TABLE otel.{table} FREEZE PARTITION '{dt}' "
                f"WITH NAME '{backup_name}'"
            )
        except Exception as e:
            log.debug("FREEZE %s partition %s: %s", table, dt, e)


def _cleanup_shadow(session_id: str):
    """Remove the shadow backup from master's data volume."""
    shadow_dir = MASTER_CH_DATA / "shadow" / f"session_{session_id}"
    if shadow_dir.exists():
        shutil.rmtree(shadow_dir)
        log.info("Cleaned up shadow backup session_%s", session_id)


# ── Part discovery + copy ────────────────────────────────────────

def _get_master_parts(ch, table: str, partition_ids: list[str]) -> list[dict]:
    """Query system.parts on master for parts in the target partitions."""
    if not partition_ids:
        return []
    id_list = ", ".join(f"'{p}'" for p in partition_ids)
    result = ch.query(
        f"SELECT name, partition, path FROM system.parts "
        f"WHERE database = 'otel' AND table = '{table}' "
        f"AND active AND partition IN ({id_list})"
    )
    return [{"name": r[0], "partition": r[1], "path": r[2]} for r in result.result_rows]


def _get_chdb_table_data_path(session_id: str, table: str) -> str:
    """Get the filesystem data path for a table in chDB session."""
    sess = _chdb_session(session_id)
    result = sess.query(
        f"SELECT data_paths FROM system.tables "
        f"WHERE database = 'default' AND name = '{table}'",
        "JSON",
    )
    import json
    parsed = json.loads(result.bytes())
    rows = parsed.get("data", [])
    if rows:
        paths = rows[0].get("data_paths", [])
        if paths:
            return paths[0]
    raise RuntimeError(f"Table {table} not found in chDB session {session_id}")


def _copy_parts(session_id: str, table: str, parts: list[dict], chdb_data_path: str) -> int:
    """Copy frozen parts from master shadow to chDB session detached dir."""
    if not parts:
        return 0

    backup_name = f"session_{session_id}"
    detached_dir = Path(chdb_data_path) / "detached"
    detached_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for part in parts:
        rel_path = part["path"]
        if rel_path.startswith("/var/lib/clickhouse/"):
            rel_path = rel_path[len("/var/lib/clickhouse/"):]
        shadow_part = MASTER_CH_DATA / "shadow" / backup_name / rel_path

        if not shadow_part.exists():
            log.warning("Shadow part not found: %s", shadow_part)
            continue

        dest = detached_dir / part["name"]
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(shadow_part, dest)
        count += 1

    log.info("Copied %d parts for %s to detached", count, table)
    return count


# ── ATTACH on chDB session ───────────────────────────────────────

def _attach_parts(session_id: str, table: str, partition_ids: list[str]):
    """Attach detached parts in chDB session."""
    sess = _chdb_session(session_id)
    for pid in partition_ids:
        try:
            sess.query(f"ALTER TABLE {table} ATTACH PARTITION ID '{pid}'")
        except Exception as e:
            log.debug("ATTACH %s partition %s: %s", table, pid, e)


# ── Manifest ─────────────────────────────────────────────────────

def _build_manifest(session_id: str) -> dict:
    """Build manifest from chDB session tables."""
    import json as _json

    sess = _chdb_session(session_id)
    tables = {}
    for table_name in TABLES:
        try:
            count_result = sess.query(f"SELECT count() FROM {table_name}", "JSON")
            count_data = _json.loads(count_result.bytes())
            count = int(count_data["data"][0]["count()"])
            if count == 0:
                continue

            cols_result = sess.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = 'default' AND table = '{table_name}' "
                f"ORDER BY position",
                "JSON",
            )
            cols_data = _json.loads(cols_result.bytes())

            sample_result = sess.query(f"SELECT * FROM {table_name} LIMIT 3", "JSON")
            sample_data = _json.loads(sample_result.bytes())
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
    """Build a session by shipping parts from master CH and loading via chDB."""
    import json as _json

    master = _master_client()

    table_map = {"traces": "otel_traces", "logs": "otel_logs", "metrics": "otel_metrics"}
    tables_to_ship = [table_map[s] for s in signal_types if s in table_map]

    date_vals = _date_values(start, end)
    partition_ids = _date_range(start, end)

    try:
        # 1. Create session dir + tables via chDB
        log.info("Creating chDB session %s", session_id)
        _session_dir(session_id).mkdir(parents=True, exist_ok=True)
        _create_session_tables(session_id, signal_types)

        # 2. FREEZE partitions on master
        for table in tables_to_ship:
            log.info("Freezing %s for %d partitions...", table, len(date_vals))
            _freeze_partitions(master, session_id, table, date_vals)

        # 3. Copy parts from master shadow → chDB detached
        for table in tables_to_ship:
            parts = _get_master_parts(master, table, partition_ids)
            if parts:
                data_path = _get_chdb_table_data_path(session_id, table)
                _copy_parts(session_id, table, parts, data_path)

        # 4. ATTACH parts in chDB session
        for table in tables_to_ship:
            log.info("Attaching %s partitions...", table)
            _attach_parts(session_id, table, partition_ids)

        # 5. Cleanup shadow on master
        _cleanup_shadow(session_id)

        # 6. Collect counts
        sess = _chdb_session(session_id)
        counts = {}
        for signal, table in table_map.items():
            if signal in signal_types:
                try:
                    r = sess.query(f"SELECT count() FROM {table}", "JSON")
                    data = _json.loads(r.bytes())
                    counts[signal] = int(data["data"][0]["count()"])
                except Exception:
                    counts[signal] = 0

        # 7. Build manifest
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
