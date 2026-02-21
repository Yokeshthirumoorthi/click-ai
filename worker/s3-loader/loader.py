"""
S3 Loader: reads OTLP JSON files from S3, unpacks envelopes into flat span rows,
bulk-inserts into ClickHouse, and tracks progress via watermark table.
"""

import json
import logging
import time
from datetime import datetime

import boto3
import clickhouse_connect
from google.protobuf.json_format import Parse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
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


def get_processed_files(ch):
    """Get set of filenames already processed (from watermark table)."""
    result = ch.query(
        "SELECT Filename FROM loader_file_watermark FINAL"
    )
    return {row[0] for row in result.result_rows}


def list_s3_files(s3):
    """List all .json files under the configured prefix."""
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=config.S3_PREFIX):
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


def unpack_otlp_json(raw_json: str) -> list[dict]:
    """
    Parse an OTLP JSON envelope into flat span rows matching the
    otel.otel_traces ClickHouse schema.
    """
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
                # Convert trace_id and span_id from bytes to hex string
                trace_id = span.trace_id.hex()
                span_id = span.span_id.hex()
                parent_span_id = span.parent_span_id.hex() if span.parent_span_id else ""

                # Timestamp: protobuf uses nanoseconds since epoch
                ts_ns = span.start_time_unix_nano
                # DateTime64(9) expects seconds with nanosecond precision
                timestamp = datetime.utcfromtimestamp(ts_ns / 1e9)

                span_attrs = extract_attributes(span.attributes)

                # Events (Nested)
                event_timestamps = []
                event_names = []
                event_attributes = []
                for event in span.events:
                    event_timestamps.append(
                        datetime.utcfromtimestamp(event.time_unix_nano / 1e9)
                    )
                    event_names.append(event.name)
                    event_attributes.append(extract_attributes(event.attributes))

                # Links (Nested)
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


COLUMNS = [
    "Timestamp", "TraceId", "SpanId", "ParentSpanId", "TraceState",
    "SpanName", "SpanKind", "ServiceName", "ResourceAttributes",
    "ScopeName", "ScopeVersion", "SpanAttributes", "Duration",
    "StatusCode", "StatusMessage",
    "Events.Timestamp", "Events.Name", "Events.Attributes",
    "Links.TraceId", "Links.SpanId", "Links.TraceState", "Links.Attributes",
]


def insert_rows(ch, rows: list[dict]):
    """Bulk insert rows into otel.otel_traces."""
    if not rows:
        return

    data = [[row[col] for col in COLUMNS] for row in rows]

    for i in range(0, len(data), config.BATCH_SIZE):
        batch = data[i : i + config.BATCH_SIZE]
        ch.insert(
            "otel_traces",
            batch,
            column_names=COLUMNS,
        )


def record_watermark(ch, filename: str, status: str, row_count: int, error_msg: str = ""):
    """Record file processing result in the watermark table."""
    ch.insert(
        "loader_file_watermark",
        [[filename, status, datetime.utcnow(), row_count, error_msg]],
        column_names=["Filename", "Status", "ProcessedAt", "RowCount", "ErrorMessage"],
    )


def process_file(s3, ch, key: str):
    """Download and process a single OTLP JSON file from S3."""
    log.info("Processing: %s", key)

    response = s3.get_object(Bucket=config.S3_BUCKET, Key=key)
    raw = response["Body"].read().decode("utf-8")

    rows = unpack_otlp_json(raw)
    insert_rows(ch, rows)
    record_watermark(ch, key, "done", len(rows))

    log.info("  → %d spans inserted", len(rows))


def run():
    log.info("S3 Loader starting")
    log.info("  S3 endpoint: %s", config.S3_ENDPOINT)
    log.info("  S3 bucket:   %s/%s", config.S3_BUCKET, config.S3_PREFIX)
    log.info("  CH host:     %s", config.CH_HOST)
    log.info("  Poll interval: %ds", config.POLL_INTERVAL)

    s3 = get_s3_client()
    ch = get_ch_client()

    while True:
        try:
            processed = get_processed_files(ch)
            all_files = list_s3_files(s3)
            new_files = [f for f in all_files if f not in processed]

            if new_files:
                log.info("Found %d new files (of %d total)", len(new_files), len(all_files))

            for key in sorted(new_files):
                try:
                    process_file(s3, ch, key)
                except Exception as e:
                    log.error("Failed to process %s: %s", key, e, exc_info=True)
                    record_watermark(ch, key, "failed", 0, str(e))

        except Exception as e:
            log.error("Poll cycle error: %s", e, exc_info=True)

        time.sleep(config.POLL_INTERVAL)


if __name__ == "__main__":
    run()
