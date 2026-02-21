"""Session builder — ships parts from master ClickHouse to session ClickHouse.

Flow: FREEZE partitions on master → copy parts to session CH detached dir → ATTACH.
Each session gets its own database (session_<id>) for isolation.
"""

import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import clickhouse_connect

from . import config

log = logging.getLogger(__name__)

# Mount points inside the platform container (set via docker-compose volumes)
MASTER_CH_DATA = Path(os.getenv("MASTER_CH_DATA", "/master-ch-data"))
SESSION_CH_DATA = Path(os.getenv("SESSION_CH_DATA", "/session-ch-data"))

TABLES = ["otel_traces", "otel_logs", "otel_metrics"]

# Table DDL for session databases — must match master's partition/order keys exactly.
# No TTL, no bloom indexes (lightweight read-only copy).
_TABLE_DDL = {
    "otel_traces": """
        CREATE TABLE IF NOT EXISTS `{db}`.otel_traces (
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
        CREATE TABLE IF NOT EXISTS `{db}`.otel_logs (
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
        CREATE TABLE IF NOT EXISTS `{db}`.otel_metrics (
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


def _session_client():
    return clickhouse_connect.get_client(
        host=config.SESSION_CH_HOST,
        port=config.SESSION_CH_PORT,
        username=config.SESSION_CH_USER,
        password=config.SESSION_CH_PASSWORD,
    )


def _session_db(session_id: str) -> str:
    return f"session_{session_id}"


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


# ── Schema setup ─────────────────────────────────────────────────

def _create_session_db(ch, session_id: str, signal_types: list[str]):
    """Create per-session database and tables on session CH."""
    db = _session_db(session_id)
    ch.command(f"CREATE DATABASE IF NOT EXISTS `{db}`")

    table_map = {"traces": "otel_traces", "logs": "otel_logs", "metrics": "otel_metrics"}
    for signal in signal_types:
        table = table_map.get(signal)
        if table and table in _TABLE_DDL:
            ch.command(_TABLE_DDL[table].format(db=db))


def _drop_session_db(ch, session_id: str):
    """Drop the per-session database and all its tables."""
    db = _session_db(session_id)
    ch.command(f"DROP DATABASE IF EXISTS `{db}`")


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
            # Partition may not exist for this date — skip silently
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


def _get_session_table_data_path(ch, session_id: str, table: str) -> str:
    """Get the filesystem data path for a table on session CH."""
    db = _session_db(session_id)
    result = ch.query(
        f"SELECT data_paths FROM system.tables "
        f"WHERE database = '{db}' AND name = '{table}'"
    )
    if result.result_rows:
        paths = result.result_rows[0][0]
        if paths:
            return paths[0]
    raise RuntimeError(f"Table {db}.{table} not found on session CH")


def _copy_parts(session_id: str, table: str, parts: list[dict], session_data_path: str) -> int:
    """Copy frozen parts from master shadow to session CH detached dir."""
    if not parts:
        return 0

    backup_name = f"session_{session_id}"

    # Translate session CH internal path to our mount point
    # session CH sees /var/lib/clickhouse/... → we see /session-ch-data/...
    detached_dir = Path(
        session_data_path.replace("/var/lib/clickhouse/", str(SESSION_CH_DATA) + "/")
    ) / "detached"
    detached_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for part in parts:
        # Master part path example: /var/lib/clickhouse/store/xxx/uuid/partname/
        # Shadow mirrors this: /var/lib/clickhouse/shadow/<backup>/store/xxx/uuid/partname/
        # Our mount: /master-ch-data/shadow/<backup>/store/xxx/uuid/partname/
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

        # Fix ownership so clickhouse user (101:101) can ATTACH
        for dirpath, _dirnames, filenames in os.walk(dest):
            os.chown(dirpath, 101, 101)
            for fn in filenames:
                os.chown(os.path.join(dirpath, fn), 101, 101)

        count += 1

    log.info("Copied %d parts for %s to detached", count, table)
    return count


# ── ATTACH on session CH ─────────────────────────────────────────

def _attach_parts(ch, session_id: str, table: str, partition_ids: list[str]):
    """Attach detached parts on session CH."""
    db = _session_db(session_id)
    for pid in partition_ids:
        try:
            ch.command(f"ALTER TABLE `{db}`.{table} ATTACH PARTITION ID '{pid}'")
        except Exception as e:
            log.debug("ATTACH %s partition %s: %s", table, pid, e)


# ── Manifest ─────────────────────────────────────────────────────

def _build_manifest(ch, session_id: str) -> dict:
    """Build manifest from session CH tables (same format as before)."""
    db = _session_db(session_id)
    tables = {}
    for table_name in TABLES:
        try:
            count = ch.query(f"SELECT count() FROM `{db}`.{table_name}").result_rows[0][0]
            if count == 0:
                continue

            cols = ch.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = '{db}' AND table = '{table_name}' "
                f"ORDER BY position"
            )

            sample = ch.query(f"SELECT * FROM `{db}`.{table_name} LIMIT 3")
            sample_rows = []
            for row in sample.result_rows[:3]:
                sample_rows.append(
                    {sample.column_names[i]: str(v) for i, v in enumerate(row)}
                )

            tables[table_name] = {
                "row_count": count,
                "columns": [{"name": r[0], "type": r[1]} for r in cols.result_rows],
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
    """Build a session by shipping parts from master to session ClickHouse."""
    master = _master_client()
    session_ch = _session_client()

    table_map = {"traces": "otel_traces", "logs": "otel_logs", "metrics": "otel_metrics"}
    tables_to_ship = [table_map[s] for s in signal_types if s in table_map]

    date_vals = _date_values(start, end)
    partition_ids = _date_range(start, end)

    try:
        # 1. Create session database + tables on session CH
        log.info("Creating session database session_%s", session_id)
        _create_session_db(session_ch, session_id, signal_types)

        # 2. FREEZE partitions on master
        for table in tables_to_ship:
            log.info("Freezing %s for %d partitions...", table, len(date_vals))
            _freeze_partitions(master, session_id, table, date_vals)

        # 3. Copy parts from master shadow → session CH detached
        for table in tables_to_ship:
            parts = _get_master_parts(master, table, partition_ids)
            if parts:
                data_path = _get_session_table_data_path(session_ch, session_id, table)
                _copy_parts(session_id, table, parts, data_path)

        # 4. ATTACH parts on session CH
        for table in tables_to_ship:
            log.info("Attaching %s partitions...", table)
            _attach_parts(session_ch, session_id, table, partition_ids)

        # 5. Cleanup shadow on master
        _cleanup_shadow(session_id)

        # 6. Collect counts
        counts = {}
        db = _session_db(session_id)
        for signal, table in table_map.items():
            if signal in signal_types:
                try:
                    r = session_ch.query(f"SELECT count() FROM `{db}`.{table}")
                    counts[signal] = r.result_rows[0][0]
                except Exception:
                    counts[signal] = 0

        # 7. Build manifest
        manifest = _build_manifest(session_ch, session_id)
        return {"counts": counts, "manifest": manifest}
    finally:
        master.close()
        session_ch.close()


def drop_session(session_id: str):
    """Drop a session's database from session ClickHouse."""
    ch = _session_client()
    try:
        _drop_session_db(ch, session_id)
        log.info("Dropped session database session_%s", session_id)
    finally:
        ch.close()


def get_available_services() -> list[str]:
    ch = _master_client()
    try:
        result = ch.query(
            "SELECT DISTINCT ServiceName FROM otel_traces ORDER BY ServiceName"
        )
        return [row[0] for row in result.result_rows]
    finally:
        ch.close()
