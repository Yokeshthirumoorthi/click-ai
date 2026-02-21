"""
OTLP gRPC writer — sends generated spans/logs/metrics through the OTEL pipeline
as protobuf messages via gRPC, same path as production telemetry.
"""

import time
from datetime import datetime
from collections import defaultdict

import grpc
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue, KeyValue, InstrumentationScope,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import (
    Span, Status, ScopeSpans, ResourceSpans, TracesData,
)
from opentelemetry.proto.logs.v1.logs_pb2 import (
    LogRecord, ScopeLogs, ResourceLogs, LogsData,
)
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    Metric, Gauge, NumberDataPoint, Sum,
    ScopeMetrics, ResourceMetrics, MetricsData,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import (
    TraceServiceStub,
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2_grpc import (
    LogsServiceStub,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2_grpc import (
    MetricsServiceStub,
)

# ── SpanKind string → protobuf enum ────────────────────────────────────
SPAN_KIND_MAP = {
    "SPAN_KIND_INTERNAL": Span.SPAN_KIND_INTERNAL,
    "SPAN_KIND_SERVER": Span.SPAN_KIND_SERVER,
    "SPAN_KIND_CLIENT": Span.SPAN_KIND_CLIENT,
    "SPAN_KIND_PRODUCER": Span.SPAN_KIND_PRODUCER,
    "SPAN_KIND_CONSUMER": Span.SPAN_KIND_CONSUMER,
    "INTERNAL": Span.SPAN_KIND_INTERNAL,
    "SERVER": Span.SPAN_KIND_SERVER,
    "CLIENT": Span.SPAN_KIND_CLIENT,
    "PRODUCER": Span.SPAN_KIND_PRODUCER,
    "CONSUMER": Span.SPAN_KIND_CONSUMER,
}

STATUS_CODE_MAP = {
    "STATUS_CODE_OK": 1,
    "STATUS_CODE_ERROR": 2,
    "STATUS_CODE_UNSET": 0,
    "OK": 1,
    "ERROR": 2,
    "UNSET": 0,
}

SEVERITY_TEXT_TO_NUMBER = {
    "TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17, "FATAL": 21,
}

METRIC_TYPE_SUM = {"Sum"}
METRIC_TYPE_GAUGE = {"Gauge"}


def _dict_to_kv(d: dict) -> list:
    """Convert a dict to a list of KeyValue protobuf objects."""
    kvs = []
    for k, v in d.items():
        if isinstance(v, bool):
            kvs.append(KeyValue(key=k, value=AnyValue(bool_value=v)))
        elif isinstance(v, int):
            kvs.append(KeyValue(key=k, value=AnyValue(int_value=v)))
        elif isinstance(v, float):
            kvs.append(KeyValue(key=k, value=AnyValue(double_value=v)))
        else:
            kvs.append(KeyValue(key=k, value=AnyValue(string_value=str(v))))
    return kvs


def _ts_to_nano(ts: datetime) -> int:
    """Convert datetime to nanoseconds since epoch."""
    return int(ts.timestamp() * 1_000_000_000)


def _hex_to_bytes(hex_str: str) -> bytes:
    """Convert hex string to bytes, padding if needed."""
    if not hex_str:
        return b""
    return bytes.fromhex(hex_str)


# ── Build protobuf messages from flat dicts ─────────────────────────────

def _spans_to_proto(spans: list[dict]) -> ExportTraceServiceRequest:
    """Convert flat span dicts into an ExportTraceServiceRequest.

    Groups spans by (ServiceName, ResourceAttributes) to build
    proper ResourceSpans → ScopeSpans → Span hierarchy.
    """
    # Group by resource identity (service name + sorted resource attrs)
    resource_groups = defaultdict(list)
    for s in spans:
        resource_key = (s["ServiceName"], tuple(sorted(s["ResourceAttributes"].items())))
        resource_groups[resource_key].append(s)

    resource_spans_list = []
    for (service_name, _), group_spans in resource_groups.items():
        resource = Resource(attributes=_dict_to_kv(group_spans[0]["ResourceAttributes"]))

        proto_spans = []
        for s in group_spans:
            start_ns = _ts_to_nano(s["Timestamp"])
            end_ns = start_ns + s["Duration"]

            span = Span(
                trace_id=_hex_to_bytes(s["TraceId"]),
                span_id=_hex_to_bytes(s["SpanId"]),
                parent_span_id=_hex_to_bytes(s["ParentSpanId"]),
                trace_state=s.get("TraceState", ""),
                name=s["SpanName"],
                kind=SPAN_KIND_MAP.get(s["SpanKind"], Span.SPAN_KIND_INTERNAL),
                start_time_unix_nano=start_ns,
                end_time_unix_nano=end_ns,
                attributes=_dict_to_kv(s.get("SpanAttributes", {})),
                status=Status(
                    code=STATUS_CODE_MAP.get(s["StatusCode"], 0),
                    message=s.get("StatusMessage", ""),
                ),
            )
            proto_spans.append(span)

        scope = InstrumentationScope(
            name=group_spans[0].get("ScopeName", "snkrs-simulator"),
            version=group_spans[0].get("ScopeVersion", "1.0.0"),
        )

        resource_spans_list.append(ResourceSpans(
            resource=resource,
            scope_spans=[ScopeSpans(scope=scope, spans=proto_spans)],
        ))

    return ExportTraceServiceRequest(resource_spans=resource_spans_list)


def _logs_to_proto(logs: list[dict]) -> ExportLogsServiceRequest:
    """Convert flat log dicts into an ExportLogsServiceRequest."""
    resource_groups = defaultdict(list)
    for l in logs:
        resource_key = (l["ServiceName"], tuple(sorted(l["ResourceAttributes"].items())))
        resource_groups[resource_key].append(l)

    resource_logs_list = []
    for (service_name, _), group_logs in resource_groups.items():
        resource = Resource(attributes=_dict_to_kv(group_logs[0]["ResourceAttributes"]))

        proto_logs = []
        for l in group_logs:
            log_record = LogRecord(
                time_unix_nano=_ts_to_nano(l["Timestamp"]),
                severity_number=l.get("SeverityNumber", SEVERITY_TEXT_TO_NUMBER.get(l.get("SeverityText", "INFO"), 9)),
                severity_text=l.get("SeverityText", "INFO"),
                body=AnyValue(string_value=l.get("Body", "")),
                attributes=_dict_to_kv(l.get("LogAttributes", {})),
                trace_id=_hex_to_bytes(l.get("TraceId", "")),
                span_id=_hex_to_bytes(l.get("SpanId", "")),
            )
            proto_logs.append(log_record)

        resource_logs_list.append(ResourceLogs(
            resource=resource,
            scope_logs=[ScopeLogs(
                scope=InstrumentationScope(name="snkrs-simulator", version="1.0.0"),
                log_records=proto_logs,
            )],
        ))

    return ExportLogsServiceRequest(resource_logs=resource_logs_list)


def _metrics_to_proto(metrics: list[dict]) -> ExportMetricsServiceRequest:
    """Convert flat metric dicts into an ExportMetricsServiceRequest."""
    resource_groups = defaultdict(list)
    for m in metrics:
        resource_key = (m["ServiceName"], tuple(sorted(m["ResourceAttributes"].items())))
        resource_groups[resource_key].append(m)

    resource_metrics_list = []
    for (service_name, _), group_metrics in resource_groups.items():
        resource = Resource(attributes=_dict_to_kv(group_metrics[0]["ResourceAttributes"]))

        # Group by metric name within each resource
        metric_name_groups = defaultdict(list)
        for m in group_metrics:
            metric_name_groups[m["MetricName"]].append(m)

        proto_metrics = []
        for metric_name, mgroup in metric_name_groups.items():
            sample = mgroup[0]
            data_points = []
            for m in mgroup:
                dp = NumberDataPoint(
                    time_unix_nano=_ts_to_nano(m["Timestamp"]),
                    as_double=float(m["Value"]),
                    attributes=_dict_to_kv(m.get("MetricAttributes", {})),
                )
                data_points.append(dp)

            if sample["MetricType"] in METRIC_TYPE_SUM:
                metric = Metric(
                    name=metric_name,
                    description=sample.get("MetricDescription", ""),
                    unit=sample.get("MetricUnit", ""),
                    sum=Sum(
                        data_points=data_points,
                        aggregation_temporality=1,  # DELTA
                        is_monotonic=True,
                    ),
                )
            else:
                metric = Metric(
                    name=metric_name,
                    description=sample.get("MetricDescription", ""),
                    unit=sample.get("MetricUnit", ""),
                    gauge=Gauge(data_points=data_points),
                )
            proto_metrics.append(metric)

        resource_metrics_list.append(ResourceMetrics(
            resource=resource,
            scope_metrics=[ScopeMetrics(
                scope=InstrumentationScope(name="snkrs-simulator", version="1.0.0"),
                metrics=proto_metrics,
            )],
        ))

    return ExportMetricsServiceRequest(resource_metrics=resource_metrics_list)


# ── OTLPWriter ──────────────────────────────────────────────────────────

class OTLPWriter:
    """Batched OTLP gRPC writer — sends spans/logs/metrics through the OTEL pipeline."""

    def __init__(self, endpoint: str = "localhost:4317", batch_size: int = 5_000):
        self.endpoint = endpoint
        self.batch_size = batch_size
        self.channel = None
        self.trace_stub = None
        self.logs_stub = None
        self.metrics_stub = None

        # Buffers
        self._spans = []
        self._logs = []
        self._metrics = []

        # Stats
        self.spans_written = 0
        self.logs_written = 0
        self.metrics_written = 0
        self._start_time = time.time()

    def connect(self):
        """Establish gRPC channel."""
        self.channel = grpc.insecure_channel(
            self.endpoint,
            options=[
                ("grpc.max_send_message_length", 64 * 1024 * 1024),
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ],
        )
        self.trace_stub = TraceServiceStub(self.channel)
        self.logs_stub = LogsServiceStub(self.channel)
        self.metrics_stub = MetricsServiceStub(self.channel)

    def close(self):
        """Flush remaining and close."""
        self.flush_all()
        if self.channel:
            self.channel.close()

    def add_spans(self, spans: list[dict]):
        """Add spans to the buffer, flushing when batch_size is reached."""
        self._spans.extend(spans)
        while len(self._spans) >= self.batch_size:
            self._flush_spans(self._spans[:self.batch_size])
            self._spans = self._spans[self.batch_size:]

    def add_logs(self, logs: list[dict]):
        """Add logs to the buffer."""
        self._logs.extend(logs)
        while len(self._logs) >= self.batch_size:
            self._flush_logs(self._logs[:self.batch_size])
            self._logs = self._logs[self.batch_size:]

    def add_metrics(self, metrics: list[dict]):
        """Add metrics to the buffer."""
        self._metrics.extend(metrics)
        while len(self._metrics) >= self.batch_size:
            self._flush_metrics(self._metrics[:self.batch_size])
            self._metrics = self._metrics[self.batch_size:]

    def flush_all(self):
        """Flush all remaining buffered data."""
        if self._spans:
            self._flush_spans(self._spans)
            self._spans = []
        if self._logs:
            self._flush_logs(self._logs)
            self._logs = []
        if self._metrics:
            self._flush_metrics(self._metrics)
            self._metrics = []

    def _flush_spans(self, batch: list[dict]):
        if not batch:
            return
        try:
            request = _spans_to_proto(batch)
            self.trace_stub.Export(request)
            self.spans_written += len(batch)
        except Exception as e:
            print(f"  [ERROR] Failed to send {len(batch)} spans: {e}")
            raise

    def _flush_logs(self, batch: list[dict]):
        if not batch:
            return
        try:
            request = _logs_to_proto(batch)
            self.logs_stub.Export(request)
            self.logs_written += len(batch)
        except Exception as e:
            print(f"  [ERROR] Failed to send {len(batch)} logs: {e}")
            raise

    def _flush_metrics(self, batch: list[dict]):
        if not batch:
            return
        try:
            request = _metrics_to_proto(batch)
            self.metrics_stub.Export(request)
            self.metrics_written += len(batch)
        except Exception as e:
            print(f"  [ERROR] Failed to send {len(batch)} metrics: {e}")
            raise

    def stats(self) -> dict:
        """Return current stats."""
        elapsed = time.time() - self._start_time
        total = self.spans_written + self.logs_written + self.metrics_written
        return {
            "spans": self.spans_written,
            "logs": self.logs_written,
            "metrics": self.metrics_written,
            "total": total,
            "elapsed_s": round(elapsed, 1),
            "rows_per_sec": round(total / max(1, elapsed)),
        }

    def print_progress(self):
        """Print a one-line progress update."""
        s = self.stats()
        print(f"  spans={s['spans']:,}  logs={s['logs']:,}  metrics={s['metrics']:,}  "
              f"total={s['total']:,}  elapsed={s['elapsed_s']}s  "
              f"rate={s['rows_per_sec']:,} rows/sec")


def worker_fn(args: tuple) -> dict:
    """Worker function for multiprocessing.

    Args:
        args: (user_chunk, time_range, drop_configs, otel_config)
            user_chunk: list of user dicts
            time_range: (start_datetime, end_datetime)
            drop_configs: list of (drop_index, product, incident, drop_start, drop_end)
            otel_config: dict with endpoint, batch_size

    Returns:
        Stats dict with counts.
    """
    user_chunk, time_range, drop_configs, otel_config = args

    # Import here to avoid issues with multiprocessing
    import random as _random
    from spans import (generate_browse_product, generate_enter_draw,
                       generate_checkout, generate_account_check, generate_feed)
    from logs import generate_logs_for_spans
    from metrics import generate_metrics_for_interval
    from inventory import StockTracker, select_drop_product
    from patterns import PATTERNS, get_incident_for_drop

    writer = OTLPWriter(
        endpoint=otel_config["endpoint"],
        batch_size=otel_config.get("batch_size", 5_000),
    )
    writer.connect()

    rng = _random.Random()
    stock_tracker = StockTracker()

    for drop_idx, product, incident, drop_start, drop_end in drop_configs:
        stock_tracker.reset_for_drop(product, rng)

        drop_duration = (drop_end - drop_start).total_seconds()

        # Phase boundaries within the drop
        pre_end = drop_start + (drop_end - drop_start) * 0.25
        live_end = drop_start + (drop_end - drop_start) * 0.75

        for user in user_chunk:
            pattern = PATTERNS.get(user["behavior_type"], PATTERNS["casual_browser"])
            sessions = rng.randint(*pattern["sessions_per_drop"])

            for _ in range(sessions):
                # Pick a random time within the drop for this session
                session_ts = drop_start + (drop_end - drop_start) * rng.random()
                progress = (session_ts - drop_start).total_seconds() / max(1, drop_duration)

                # Determine phase
                if session_ts < pre_end:
                    phase = "pre_drop"
                    actions = pattern.get("pre_drop", [])
                elif session_ts < live_end:
                    phase = "drop_live"
                    actions = pattern.get("drop_live", [])
                else:
                    phase = "post_drop"
                    actions = pattern.get("post_drop", [])

                action_ts = session_ts
                for endpoint, config in actions:
                    # Check probability
                    if rng.random() > config.get("probability", 1.0):
                        continue

                    # Repeat
                    repeat_range = config.get("repeat", (1, 1))
                    if isinstance(repeat_range, int):
                        repeats = repeat_range
                    else:
                        repeats = rng.randint(*repeat_range)

                    for _ in range(repeats):
                        # Generate spans
                        spans = _generate_flow(
                            endpoint, user, product, action_ts,
                            stock_tracker, incident, progress, rng)

                        if spans:
                            writer.add_spans(spans)

                            # Generate correlated logs
                            logs = generate_logs_for_spans(spans, user, product, rng)
                            if logs:
                                writer.add_logs(logs)

                        # Advance time
                        gap_ms = rng.randint(*pattern["session_gap_ms"])
                        from datetime import timedelta
                        action_ts = action_ts + timedelta(milliseconds=gap_ms)

    writer.flush_all()
    stats = writer.stats()
    writer.close()
    return stats


def _generate_flow(endpoint: str, user: dict, product: dict, ts,
                   stock_tracker, incident, progress, rng):
    """Generate the appropriate span tree for an endpoint."""
    from spans import (generate_browse_product, generate_enter_draw,
                       generate_checkout, generate_account_check, generate_feed)

    if endpoint == "GET /v1/products/{id}":
        return generate_browse_product(user, product, ts, stock_tracker,
                                       incident, progress, rng)
    elif endpoint == "POST /v1/draw/enter":
        return generate_enter_draw(user, product, ts, stock_tracker,
                                   incident, progress, rng)
    elif endpoint == "POST /v1/checkout":
        return generate_checkout(user, product, ts, stock_tracker,
                                 incident, progress, rng)
    elif endpoint == "GET /v1/account/profile":
        return generate_account_check(user, ts, incident, progress, rng)
    elif endpoint == "GET /v1/feed":
        return generate_feed(user, ts, incident, progress, rng)
    return []
