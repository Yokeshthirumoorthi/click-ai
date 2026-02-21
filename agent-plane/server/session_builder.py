"""C6: Pulls scoped data from ClickHouse into a per-session DuckDB file."""

import json
import logging
from datetime import datetime
from pathlib import Path

import clickhouse_connect
import duckdb

from . import config

log = logging.getLogger(__name__)


def _ch_client():
    return clickhouse_connect.get_client(
        host=config.CH_HOST,
        port=config.CH_PORT,
        username=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DATABASE,
    )


def _create_duckdb_tables(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS traces (
            Timestamp VARCHAR,
            TraceId VARCHAR,
            SpanId VARCHAR,
            ParentSpanId VARCHAR,
            SpanName VARCHAR,
            SpanKind VARCHAR,
            ServiceName VARCHAR,
            Duration BIGINT,
            StatusCode VARCHAR,
            StatusMessage VARCHAR,
            SpanAttributes VARCHAR,
            ResourceAttributes VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            Timestamp VARCHAR,
            TraceId VARCHAR,
            SpanId VARCHAR,
            SeverityNumber INTEGER,
            SeverityText VARCHAR,
            Body VARCHAR,
            ServiceName VARCHAR,
            LogAttributes VARCHAR,
            ResourceAttributes VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            Timestamp VARCHAR,
            MetricName VARCHAR,
            MetricDescription VARCHAR,
            MetricUnit VARCHAR,
            MetricType VARCHAR,
            Value DOUBLE,
            ServiceName VARCHAR,
            MetricAttributes VARCHAR,
            ResourceAttributes VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS traces_enriched (
            Timestamp VARCHAR,
            TraceId VARCHAR,
            SpanId VARCHAR,
            ParentSpanId VARCHAR,
            SpanName VARCHAR,
            SpanKind VARCHAR,
            ServiceName VARCHAR,
            Duration BIGINT,
            StatusCode VARCHAR,
            StatusMessage VARCHAR,
            EmbeddingText VARCHAR,
            Embedding FLOAT[]
        )
    """)


def _pull_traces(ch, con, services: list[str], start: datetime, end: datetime):
    svc_filter = "AND ServiceName IN %(services)s" if services else ""
    query = f"""
        SELECT
            toString(Timestamp), TraceId, SpanId, ParentSpanId,
            SpanName, SpanKind, ServiceName, Duration,
            StatusCode, StatusMessage,
            toString(SpanAttributes), toString(ResourceAttributes)
        FROM otel_traces
        WHERE Timestamp >= %(start)s AND Timestamp <= %(end)s
        {svc_filter}
        ORDER BY Timestamp
        LIMIT %(limit)s
    """
    params = {"start": start, "end": end, "limit": config.MAX_ROWS_PER_TABLE}
    if services:
        params["services"] = services
    result = ch.query(query, parameters=params)
    if result.result_rows:
        con.executemany(
            "INSERT INTO traces VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            result.result_rows,
        )
    return len(result.result_rows)


def _pull_logs(ch, con, services: list[str], start: datetime, end: datetime):
    svc_filter = "AND ServiceName IN %(services)s" if services else ""
    query = f"""
        SELECT
            toString(Timestamp), TraceId, SpanId,
            SeverityNumber, SeverityText, Body, ServiceName,
            toString(LogAttributes), toString(ResourceAttributes)
        FROM otel_logs
        WHERE Timestamp >= %(start)s AND Timestamp <= %(end)s
        {svc_filter}
        ORDER BY Timestamp
        LIMIT %(limit)s
    """
    params = {"start": start, "end": end, "limit": config.MAX_ROWS_PER_TABLE}
    if services:
        params["services"] = services
    result = ch.query(query, parameters=params)
    if result.result_rows:
        con.executemany(
            "INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            result.result_rows,
        )
    return len(result.result_rows)


def _pull_metrics(ch, con, services: list[str], start: datetime, end: datetime):
    svc_filter = "AND ServiceName IN %(services)s" if services else ""
    query = f"""
        SELECT
            toString(Timestamp), MetricName, MetricDescription,
            MetricUnit, MetricType, Value, ServiceName,
            toString(MetricAttributes), toString(ResourceAttributes)
        FROM otel_metrics
        WHERE Timestamp >= %(start)s AND Timestamp <= %(end)s
        {svc_filter}
        ORDER BY Timestamp
        LIMIT %(limit)s
    """
    params = {"start": start, "end": end, "limit": config.MAX_ROWS_PER_TABLE}
    if services:
        params["services"] = services
    result = ch.query(query, parameters=params)
    if result.result_rows:
        con.executemany(
            "INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            result.result_rows,
        )
    return len(result.result_rows)


def _pull_enriched(ch, con, services: list[str], start: datetime, end: datetime):
    svc_filter = "AND ServiceName IN %(services)s" if services else ""
    query = f"""
        SELECT
            toString(Timestamp), TraceId, SpanId, ParentSpanId,
            SpanName, SpanKind, ServiceName, Duration,
            StatusCode, StatusMessage,
            EmbeddingText, Embedding
        FROM otel_traces_enriched
        WHERE Timestamp >= %(start)s AND Timestamp <= %(end)s
        {svc_filter}
        ORDER BY Timestamp
        LIMIT %(limit)s
    """
    params = {"start": start, "end": end, "limit": config.MAX_ROWS_PER_TABLE}
    if services:
        params["services"] = services
    result = ch.query(query, parameters=params)
    if result.result_rows:
        con.executemany(
            "INSERT INTO traces_enriched VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            result.result_rows,
        )
    return len(result.result_rows)


def _build_manifest(con: duckdb.DuckDBPyConnection) -> dict:
    tables = {}
    for table_name in ["traces", "logs", "metrics", "traces_enriched"]:
        try:
            count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            if count == 0:
                continue
            cols = con.execute(f"DESCRIBE {table_name}").fetchall()
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
            tables[table_name] = {
                "row_count": count,
                "columns": [{"name": c[0], "type": c[1]} for c in cols],
                "sample_rows": json.loads(sample.to_json(orient="records")),
            }
        except Exception:
            continue
    return tables


def build_session(
    db_path: Path,
    services: list[str],
    signal_types: list[str],
    start: datetime,
    end: datetime,
) -> dict:
    ch = _ch_client()
    con = duckdb.connect(str(db_path))

    try:
        _create_duckdb_tables(con)
        counts = {}

        if "traces" in signal_types:
            counts["traces"] = _pull_traces(ch, con, services, start, end)
            counts["traces_enriched"] = _pull_enriched(ch, con, services, start, end)
        if "logs" in signal_types:
            counts["logs"] = _pull_logs(ch, con, services, start, end)
        if "metrics" in signal_types:
            counts["metrics"] = _pull_metrics(ch, con, services, start, end)

        manifest = _build_manifest(con)
        return {"counts": counts, "manifest": manifest}
    finally:
        con.close()
        ch.close()


def get_available_services() -> list[str]:
    ch = _ch_client()
    try:
        result = ch.query("SELECT DISTINCT ServiceName FROM otel_traces ORDER BY ServiceName")
        return [row[0] for row in result.result_rows]
    finally:
        ch.close()
