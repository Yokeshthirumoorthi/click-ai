"""
Event Generator — runs inside Docker, sends fake OTEL events.
All config comes from environment variables set in docker-compose.
"""

import os
import time
import random
from datetime import datetime

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
import logging

# ── Config from environment variables ──────────────────────
ENDPOINT         = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
EVENTS_PER_SEC   = int(os.getenv("EVENTS_PER_SECOND", "20"))
SLEEP_INTERVAL   = 1.0 / EVENTS_PER_SEC

print(f"Starting event generator")
print(f"  → Sending to:  {ENDPOINT}")
print(f"  → Rate:        {EVENTS_PER_SEC} events/sec")
print(f"  → Check data:  http://localhost:8080 (Tabix)")
print()

# ── App metadata ────────────────────────────────────────────
resource = Resource.create({
    "service.name":           "event-generator",
    "service.version":        "1.0.0",
    "deployment.environment": "docker",
})

# ── Traces setup ────────────────────────────────────────────
trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=ENDPOINT, insecure=True))
)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer("event-generator")

# ── Metrics setup ───────────────────────────────────────────
reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=ENDPOINT, insecure=True),
    export_interval_millis=5000,
)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("event-generator")

request_counter  = meter.create_counter("http.requests",         description="Total HTTP requests")
response_latency = meter.create_histogram("http.response_time_ms", description="Response time in ms")
active_users     = meter.create_up_down_counter("app.active_users", description="Active users")

# ── Logs setup ──────────────────────────────────────────────
log_provider = LoggerProvider(resource=resource)
log_provider.add_log_record_processor(
    BatchLogRecordProcessor(OTLPLogExporter(endpoint=ENDPOINT, insecure=True))
)
set_logger_provider(log_provider)

otel_handler = LoggingHandler(level=logging.DEBUG, logger_provider=log_provider)
logger = logging.getLogger("event-generator")
logger.addHandler(otel_handler)
logger.setLevel(logging.DEBUG)

# ── Simulate a realistic web service ───────────────────────
ENDPOINTS = [
    ("/api/users",    "GET",  200, 80),   # (path, method, status, base_latency_ms)
    ("/api/orders",   "POST", 201, 150),
    ("/api/products", "GET",  200, 60),
    ("/api/checkout", "POST", 200, 300),
    ("/api/search",   "GET",  200, 200),
    ("/api/users",    "GET",  404, 20),   # occasional 404
    ("/api/orders",   "POST", 500, 50),   # occasional 500
    ("/api/checkout", "POST", 400, 30),   # occasional bad request
]

# Weight distribution: mostly successes, few errors
WEIGHTS = [30, 25, 20, 15, 5, 2, 1, 2]

event_count = 0
start_time  = time.time()

while True:
    path, method, status, base_latency = random.choices(ENDPOINTS, weights=WEIGHTS, k=1)[0]
    latency_ms = max(1, random.gauss(base_latency, base_latency * 0.2))
    user_id    = random.randint(1, 50000)
    region     = random.choice(["us-east", "eu-west", "ap-south"])

    # ── Trace ───────────────────────────────────────────────
    with tracer.start_as_current_span(f"{method} {path}") as span:
        span.set_attribute("http.method",      method)
        span.set_attribute("http.route",       path)
        span.set_attribute("http.status_code", status)
        span.set_attribute("user.id",          user_id)
        span.set_attribute("cloud.region",     region)
        span.set_attribute("response_time_ms", round(latency_ms, 2))
        if status >= 500:
            span.set_attribute("error", True)
            span.record_exception(Exception(f"Server error on {path}"))

    # ── Metrics ─────────────────────────────────────────────
    labels = {"endpoint": path, "method": method, "status": str(status), "region": region}
    request_counter.add(1, labels)
    response_latency.record(latency_ms, labels)
    active_users.add(random.choice([-2, -1, 0, 0, 1, 1, 2, 3]))

    # ── Log ─────────────────────────────────────────────────
    msg = f"{method} {path} {status} {latency_ms:.1f}ms user={user_id} region={region}"
    if status >= 500:
        logger.error(msg)
    elif status >= 400:
        logger.warning(msg)
    else:
        logger.info(msg)

    event_count += 1

    # Print a status line every 500 events
    if event_count % 500 == 0:
        elapsed = time.time() - start_time
        rate    = event_count / elapsed
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent {event_count:,} events  ({rate:.1f}/sec)")

    time.sleep(SLEEP_INTERVAL)