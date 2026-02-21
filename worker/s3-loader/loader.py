"""
S3 Loader: reads OTLP JSON files from S3, unpacks traces/metrics/logs envelopes
into flat rows, bulk-inserts into ClickHouse, and tracks progress via watermark tables.

Architecture:
  Main thread
  ├── Signal thread: traces    ─── ThreadPoolExecutor(N) ─── download + parse files
  ├── Signal thread: logs      ─── ThreadPoolExecutor(N) ─── download + parse files
  └── Signal thread: metrics   ─── ThreadPoolExecutor(N) ─── download + parse files

Each signal pipeline runs independently with adaptive polling (fast when busy, slow when idle).
"""

import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
import clickhouse_connect
from google.protobuf.json_format import Parse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("s3-loader")

# ── SpanKind enum → string mapping ─────────────────────────────
SPAN_KIND_MAP = {
    0: "SPAN_KIND_UNSPECIFIED",
    1: "SPAN_KIND_INTERNAL",
    2: "SPAN_KIND_SERVER",
    3: "SPAN_KIND_CLIENT",
    4: "SPAN_KIND_PRODUCER",
    5: "SPAN_KIND_CONSUMER",
}

STATUS_CODE_MAP = {
    0: "STATUS_CODE_UNSET",
    1: "STATUS_CODE_OK",
    2: "STATUS_CODE_ERROR",
}

SEVERITY_NUMBER_TO_TEXT = {
    0: "UNSPECIFIED", 1: "TRACE", 2: "TRACE2", 3: "TRACE3", 4: "TRACE4",
    5: "DEBUG", 6: "DEBUG2", 7: "DEBUG3", 8: "DEBUG4",
    9: "INFO", 10: "INFO2", 11: "INFO3", 12: "INFO4",
    13: "WARN", 14: "WARN2", 15: "WARN3", 16: "WARN4",
    17: "ERROR", 18: "ERROR2", 19: "ERROR3", 20: "ERROR4",
    21: "FATAL", 22: "FATAL2", 23: "FATAL3", 24: "FATAL4",
}

METRIC_TYPE_MAP = {
    "gauge": "Gauge",
    "sum": "Sum",
    "histogram": "Histogram",
    "exponential_histogram": "ExponentialHistogram",
    "summary": "Summary",
}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name="us-east-1",
    )


def get_ch_client():
    return clickhouse_connect.get_client(
        host=config.CH_HOST,
        port=8123,
        username=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DATABASE,
    )


def get_processed_files(ch, watermark_table: str):
    """Get set of filenames already processed (from watermark table)."""
    result = ch.query(f"SELECT Filename FROM {watermark_table} FINAL")
    return {row[0] for row in result.result_rows}


def list_s3_files(s3, prefix: str):
    """List all .json files under the given prefix."""
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                files.append(key)
    return files


def extract_attributes(attrs_list):
    """Convert protobuf attribute list to a dict of string key → string value."""
    result = {}
    for attr in attrs_list:
        key = attr.key
        val = attr.value
        if val.HasField("string_value"):
            result[key] = val.string_value
        elif val.HasField("int_value"):
            result[key] = str(val.int_value)
        elif val.HasField("double_value"):
            result[key] = str(val.double_value)
        elif val.HasField("bool_value"):
            result[key] = str(val.bool_value).lower()
        else:
            result[key] = str(val)
    return result


# ── Traces ─────────────────────────────────────────────────────

def unpack_otlp_traces_json(raw_json: str) -> list[dict]:
    """Parse an OTLP JSON envelope into flat span rows."""
    request = ExportTraceServiceRequest()
    Parse(raw_json, request)

    rows = []
    for resource_spans in request.resource_spans:
        resource_attrs = extract_attributes(resource_spans.resource.attributes)
        service_name = resource_attrs.get("service.name", "unknown")

        for scope_spans in resource_spans.scope_spans:
            scope_name = scope_spans.scope.name if scope_spans.scope.name else ""
            scope_version = scope_spans.scope.version if scope_spans.scope.version else ""

            for span in scope_spans.spans:
                trace_id = span.trace_id.hex()
                span_id = span.span_id.hex()
                parent_span_id = span.parent_span_id.hex() if span.parent_span_id else ""

                ts_ns = span.start_time_unix_nano
                timestamp = datetime.utcfromtimestamp(ts_ns / 1e9)

                span_attrs = extract_attributes(span.attributes)

                event_timestamps = []
                event_names = []
                event_attributes = []
                for event in span.events:
                    event_timestamps.append(
                        datetime.utcfromtimestamp(event.time_unix_nano / 1e9)
                    )
                    event_names.append(event.name)
                    event_attributes.append(extract_attributes(event.attributes))

                link_trace_ids = []
                link_span_ids = []
                link_trace_states = []
                link_attributes = []
                for link in span.links:
                    link_trace_ids.append(link.trace_id.hex())
                    link_span_ids.append(link.span_id.hex())
                    link_trace_states.append(link.trace_state)
                    link_attributes.append(extract_attributes(link.attributes))

                duration_ns = span.end_time_unix_nano - span.start_time_unix_nano

                rows.append({
                    "Timestamp": timestamp,
                    "TraceId": trace_id,
                    "SpanId": span_id,
                    "ParentSpanId": parent_span_id,
                    "TraceState": span.trace_state,
                    "SpanName": span.name,
                    "SpanKind": SPAN_KIND_MAP.get(span.kind, "SPAN_KIND_UNSPECIFIED"),
                    "ServiceName": service_name,
                    "ResourceAttributes": resource_attrs,
                    "ScopeName": scope_name,
                    "ScopeVersion": scope_version,
                    "SpanAttributes": span_attrs,
                    "Duration": max(0, duration_ns),
                    "StatusCode": STATUS_CODE_MAP.get(
                        span.status.code, "STATUS_CODE_UNSET"
                    ),
                    "StatusMessage": span.status.message,
                    "Events.Timestamp": event_timestamps,
                    "Events.Name": event_names,
                    "Events.Attributes": event_attributes,
                    "Links.TraceId": link_trace_ids,
                    "Links.SpanId": link_span_ids,
                    "Links.TraceState": link_trace_states,
                    "Links.Attributes": link_attributes,
                })

    return rows


TRACE_COLUMNS = [
    "Timestamp", "TraceId", "SpanId", "ParentSpanId", "TraceState",
    "SpanName", "SpanKind", "ServiceName", "ResourceAttributes",
    "ScopeName", "ScopeVersion", "SpanAttributes", "Duration",
    "StatusCode", "StatusMessage",
    "Events.Timestamp", "Events.Name", "Events.Attributes",
    "Links.TraceId", "Links.SpanId", "Links.TraceState", "Links.Attributes",
]


# ── Logs ───────────────────────────────────────────────────────

def unpack_otlp_logs_json(raw_json: str) -> list[dict]:
    """Parse an OTLP JSON envelope into flat log rows."""
    request = ExportLogsServiceRequest()
    Parse(raw_json, request)

    rows = []
    for resource_logs in request.resource_logs:
        resource_attrs = extract_attributes(resource_logs.resource.attributes)
        service_name = resource_attrs.get("service.name", "unknown")

        for scope_logs in resource_logs.scope_logs:
            for log_record in scope_logs.log_records:
                ts_ns = log_record.time_unix_nano
                timestamp = datetime.utcfromtimestamp(ts_ns / 1e9) if ts_ns else datetime.utcfromtimestamp(0)

                trace_id = log_record.trace_id.hex() if log_record.trace_id else ""
                span_id = log_record.span_id.hex() if log_record.span_id else ""

                severity_number = log_record.severity_number
                severity_text = log_record.severity_text or SEVERITY_NUMBER_TO_TEXT.get(severity_number, "UNSPECIFIED")

                body = ""
                if log_record.body.HasField("string_value"):
                    body = log_record.body.string_value
                else:
                    body = str(log_record.body)

                log_attrs = extract_attributes(log_record.attributes)

                rows.append({
                    "Timestamp": timestamp,
                    "TraceId": trace_id,
                    "SpanId": span_id,
                    "SeverityNumber": severity_number,
                    "SeverityText": severity_text,
                    "Body": body,
                    "ServiceName": service_name,
                    "ResourceAttributes": resource_attrs,
                    "LogAttributes": log_attrs,
                })

    return rows


LOG_COLUMNS = [
    "Timestamp", "TraceId", "SpanId", "SeverityNumber", "SeverityText",
    "Body", "ServiceName", "ResourceAttributes", "LogAttributes",
]


# ── Metrics ────────────────────────────────────────────────────

def _extract_data_points(metric) -> list[tuple]:
    """
    Extract (timestamp, value) pairs from any metric type.
    Returns list of (datetime, float) tuples.
    """
    points = []

    if metric.HasField("gauge"):
        for dp in metric.gauge.data_points:
            ts = datetime.utcfromtimestamp(dp.time_unix_nano / 1e9) if dp.time_unix_nano else datetime.utcfromtimestamp(0)
            val = dp.as_double if dp.as_double else float(dp.as_int)
            attrs = extract_attributes(dp.attributes)
            points.append((ts, val, attrs))
    elif metric.HasField("sum"):
        for dp in metric.sum.data_points:
            ts = datetime.utcfromtimestamp(dp.time_unix_nano / 1e9) if dp.time_unix_nano else datetime.utcfromtimestamp(0)
            val = dp.as_double if dp.as_double else float(dp.as_int)
            attrs = extract_attributes(dp.attributes)
            points.append((ts, val, attrs))
    elif metric.HasField("histogram"):
        for dp in metric.histogram.data_points:
            ts = datetime.utcfromtimestamp(dp.time_unix_nano / 1e9) if dp.time_unix_nano else datetime.utcfromtimestamp(0)
            val = dp.sum if dp.sum else 0.0
            attrs = extract_attributes(dp.attributes)
            points.append((ts, val, attrs))
    elif metric.HasField("summary"):
        for dp in metric.summary.data_points:
            ts = datetime.utcfromtimestamp(dp.time_unix_nano / 1e9) if dp.time_unix_nano else datetime.utcfromtimestamp(0)
            val = dp.sum if dp.sum else 0.0
            attrs = extract_attributes(dp.attributes)
            points.append((ts, val, attrs))

    return points


def _get_metric_type(metric) -> str:
    for field_name in METRIC_TYPE_MAP:
        if metric.HasField(field_name):
            return METRIC_TYPE_MAP[field_name]
    return "Unknown"


def unpack_otlp_metrics_json(raw_json: str) -> list[dict]:
    """Parse an OTLP JSON envelope into flat metric rows."""
    request = ExportMetricsServiceRequest()
    Parse(raw_json, request)

    rows = []
    for resource_metrics in request.resource_metrics:
        resource_attrs = extract_attributes(resource_metrics.resource.attributes)
        service_name = resource_attrs.get("service.name", "unknown")

        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                metric_type = _get_metric_type(metric)
                data_points = _extract_data_points(metric)

                for ts, val, dp_attrs in data_points:
                    rows.append({
                        "Timestamp": ts,
                        "MetricName": metric.name,
                        "MetricDescription": metric.description,
                        "MetricUnit": metric.unit,
                        "MetricType": metric_type,
                        "Value": val,
                        "ServiceName": service_name,
                        "ResourceAttributes": resource_attrs,
                        "MetricAttributes": dp_attrs,
                    })

    return rows


METRIC_COLUMNS = [
    "Timestamp", "MetricName", "MetricDescription", "MetricUnit",
    "MetricType", "Value", "ServiceName", "ResourceAttributes", "MetricAttributes",
]


# ── Columnar insert helpers ────────────────────────────────────

def _rows_to_columns(rows: list[dict], columns: list[str]) -> list[list]:
    """Convert list of row dicts to columnar format for clickhouse-connect."""
    return [[row[col] for row in rows] for col in columns]


def insert_rows(ch, table: str, columns: list[str], rows: list[dict]):
    """Bulk insert rows into a ClickHouse table using columnar format."""
    if not rows:
        return

    for i in range(0, len(rows), config.BATCH_SIZE):
        batch = rows[i : i + config.BATCH_SIZE]
        col_data = _rows_to_columns(batch, columns)
        ch.insert(table, col_data, column_names=columns, column_oriented=True)


def record_watermark(ch, watermark_table: str, filename: str, status: str, row_count: int, error_msg: str = ""):
    """Record file processing result in the watermark table."""
    ch.insert(
        watermark_table,
        [[filename, status, datetime.utcnow(), row_count, error_msg]],
        column_names=["Filename", "Status", "ProcessedAt", "RowCount", "ErrorMessage"],
    )


# ── Signal pipeline definitions ────────────────────────────────

SIGNAL_PIPELINES = [
    {
        "name": "traces",
        "s3_prefix": config.S3_TRACES_PREFIX,
        "watermark_table": "loader_file_watermark",
        "ch_table": "otel_traces",
        "columns": TRACE_COLUMNS,
        "unpack_fn": unpack_otlp_traces_json,
        "row_label": "spans",
    },
    {
        "name": "logs",
        "s3_prefix": config.S3_LOGS_PREFIX,
        "watermark_table": "log_loader_file_watermark",
        "ch_table": "otel_logs",
        "columns": LOG_COLUMNS,
        "unpack_fn": unpack_otlp_logs_json,
        "row_label": "log records",
    },
    {
        "name": "metrics",
        "s3_prefix": config.S3_METRICS_PREFIX,
        "watermark_table": "metric_loader_file_watermark",
        "ch_table": "otel_metrics",
        "columns": METRIC_COLUMNS,
        "unpack_fn": unpack_otlp_metrics_json,
        "row_label": "data points",
    },
]


# ── File processing with concurrent downloads ──────────────────

def download_and_parse(s3, key: str, unpack_fn) -> tuple[str, list[dict]]:
    """Download a single S3 file and parse it. Returns (key, rows)."""
    response = s3.get_object(Bucket=config.S3_BUCKET, Key=key)
    raw = response["Body"].read().decode("utf-8")
    rows = unpack_fn(raw)
    return key, rows


def process_files_concurrent(s3, ch, new_files: list[str], pipeline: dict):
    """Process a batch of new files using concurrent downloads."""
    total_rows = 0
    with ThreadPoolExecutor(max_workers=config.MAX_FILE_WORKERS) as executor:
        futures = {
            executor.submit(download_and_parse, s3, key, pipeline["unpack_fn"]): key
            for key in sorted(new_files)
        }

        for future in as_completed(futures):
            key = futures[future]
            try:
                _, rows = future.result()
                insert_rows(ch, pipeline["ch_table"], pipeline["columns"], rows)
                record_watermark(ch, pipeline["watermark_table"], key, "done", len(rows))
                total_rows += len(rows)
                log.info("[%s]   → %d %s from %s", pipeline["name"], len(rows), pipeline["row_label"], key)
            except Exception as e:
                log.error("[%s] Failed to process %s: %s", pipeline["name"], key, e, exc_info=True)
                record_watermark(ch, pipeline["watermark_table"], key, "failed", 0, str(e))

    return total_rows


def signal_loop(pipeline: dict):
    """Continuous polling loop for a single signal type. Runs in its own thread."""
    name = pipeline["name"]
    log.info("[%s] Signal thread starting", name)

    # Each thread gets its own S3 and CH clients (not thread-safe to share)
    s3 = get_s3_client()
    ch = get_ch_client()

    while True:
        try:
            processed = get_processed_files(ch, pipeline["watermark_table"])
            all_files = list_s3_files(s3, pipeline["s3_prefix"])
            new_files = [f for f in all_files if f not in processed]

            if new_files:
                log.info("[%s] Found %d new files (of %d total)", name, len(new_files), len(all_files))
                total = process_files_concurrent(s3, ch, new_files, pipeline)
                log.info("[%s] Batch complete: %d total %s inserted", name, total, pipeline["row_label"])
                time.sleep(config.POLL_INTERVAL_BUSY)
            else:
                time.sleep(config.POLL_INTERVAL_IDLE)

        except Exception as e:
            log.error("[%s] Poll cycle error: %s", name, e, exc_info=True)
            time.sleep(config.POLL_INTERVAL_IDLE)


def run():
    log.info("S3 Loader starting (concurrent mode)")
    log.info("  S3 endpoint: %s", config.S3_ENDPOINT)
    log.info("  S3 bucket:   %s", config.S3_BUCKET)
    log.info("  Prefixes:    traces=%s  metrics=%s  logs=%s",
             config.S3_TRACES_PREFIX, config.S3_METRICS_PREFIX, config.S3_LOGS_PREFIX)
    log.info("  CH host:     %s", config.CH_HOST)
    log.info("  Batch size:  %d rows/INSERT", config.BATCH_SIZE)
    log.info("  File workers: %d per signal", config.MAX_FILE_WORKERS)
    log.info("  Poll: %.1fs busy, %.1fs idle", config.POLL_INTERVAL_BUSY, config.POLL_INTERVAL_IDLE)

    threads = []
    for pipeline in SIGNAL_PIPELINES:
        t = threading.Thread(target=signal_loop, args=(pipeline,), daemon=True, name=f"signal-{pipeline['name']}")
        t.start()
        threads.append(t)
        log.info("Started signal thread: %s", pipeline["name"])

    # Keep main thread alive
    for t in threads:
        t.join()


if __name__ == "__main__":
    run()
