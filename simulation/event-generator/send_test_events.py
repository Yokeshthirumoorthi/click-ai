"""
Nike SNKRS Load Simulator
=========================
Simulates the microservice architecture of Nike's SNKRS app
during a sneaker drop event.

Architecture simulated:
                                         ┌─────────────────┐
                                         │   API Gateway   │
                                         └────────┬────────┘
                    ┌───────────────────┬─────────┴──────────┬──────────────────┐
             ┌──────▼──────┐   ┌────────▼───────┐   ┌───────▼──────┐   ┌───────▼──────┐
             │ Auth Service│   │ Product Service │   │ Draw Service │   │ Feed Service │
             └──────┬──────┘   └────────┬───────┘   └───────┬──────┘   └───────┬──────┘
                    │                   │                    │                   │
             ┌──────▼──────┐   ┌────────▼───────┐   ┌───────▼──────┐   ┌───────▼──────┐
             │ User DB     │   │ Inventory Svc  │   │ Queue Svc    │   │ CDN / Cache  │
             └─────────────┘   └────────┬───────┘   └───────┬──────┘   └─────────────┘
                                        │                    │
                               ┌────────▼───────┐   ┌───────▼──────┐
                               │ Payment Service│   │ Order Service│
                               └────────┬───────┘   └───────┬──────┘
                                        │                    │
                               ┌────────▼───────┐   ┌───────▼──────┐
                               │ Payment Gateway│   │ Notify Service│
                               └────────────────┘   └──────────────┘

Phases:
  1. PRE-DROP  — browsing, account checks, low traffic
  2. DROP LIVE — massive spike, inventory race, queue chaos
  3. SOLD OUT  — traffic dies, notifications, order fulfillment
"""

import os
import time
import random
import threading
from datetime import datetime
from enum import Enum

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

# ── Config ───────────────────────────────────────────────────────────
ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")

# Drop simulation timing (seconds)
PRE_DROP_DURATION  = 60   # 1 min of calm browsing
DROP_DURATION      = 90   # 1.5 min of absolute chaos
POST_DROP_DURATION = 60   # 1 min of cooldown

# Concurrency
PRE_DROP_WORKERS  = 5
DROP_WORKERS      = 40   # 40 concurrent users hammering the system
POST_DROP_WORKERS = 8

print("=" * 60)
print("  Nike SNKRS Drop Simulator")
print("=" * 60)
print(f"  Collector: {ENDPOINT}")
print(f"  Phase 1 — Pre-Drop:  {PRE_DROP_DURATION}s  ({PRE_DROP_WORKERS} concurrent users)")
print(f"  Phase 2 — DROP LIVE: {DROP_DURATION}s  ({DROP_WORKERS} concurrent users)")
print(f"  Phase 3 — Sold Out:  {POST_DROP_DURATION}s  ({POST_DROP_WORKERS} concurrent users)")
print("=" * 60)
time.sleep(5)

# ── OTEL Setup ───────────────────────────────────────────────────────
resource = Resource.create({
    "service.name":           "snkrs-platform",
    "service.version":        "4.2.1",
    "deployment.environment": "production",
    "cloud.provider":         "aws",
    "cloud.region":           "us-east-1",
})

trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=ENDPOINT, insecure=True))
)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer("snkrs-simulator")

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=ENDPOINT, insecure=True),
    export_interval_millis=3000,
)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("snkrs-simulator")

# Metrics
req_counter        = meter.create_counter("snkrs.requests",            description="Total API requests")
inventory_gauge    = meter.create_up_down_counter("snkrs.inventory",   description="Remaining inventory")
queue_depth        = meter.create_up_down_counter("snkrs.queue_depth", description="Draw queue depth")
error_counter      = meter.create_counter("snkrs.errors",              description="Total errors")
payment_counter    = meter.create_counter("snkrs.payments",            description="Payment attempts")

log_provider = LoggerProvider(resource=resource)
log_provider.add_log_record_processor(
    BatchLogRecordProcessor(OTLPLogExporter(endpoint=ENDPOINT, insecure=True))
)
set_logger_provider(log_provider)
otel_handler = LoggingHandler(level=logging.DEBUG, logger_provider=log_provider)
logger = logging.getLogger("snkrs")
logger.addHandler(otel_handler)
logger.setLevel(logging.DEBUG)

# ── Shared state (simulates real system state) ────────────────────────
class DropState:
    inventory     = 500      # limited pairs available
    queue_entries = 0
    phase         = "PRE_DROP"
    lock          = threading.Lock()

state = DropState()

# ── Sneaker data ──────────────────────────────────────────────────────
SNEAKERS = [
    {"id": "AJ1-001", "name": "Air Jordan 1 Retro High OG 'Chicago'",   "price": 180, "sku": "555088-101"},
    {"id": "YZ-350",  "name": "Yeezy Boost 350 V2 'Zebra'",             "price": 220, "sku": "CP9654"},
    {"id": "DF-001",  "name": "Nike Dunk Low 'Panda'",                  "price": 110, "sku": "DD1391-100"},
    {"id": "AF1-007", "name": "Air Force 1 Low '07 'Triple White'",     "price": 90,  "sku": "CW2288-111"},
]
DROP_SNEAKER = SNEAKERS[0]  # the one being dropped today

REGIONS = ["us-east", "us-west", "eu-west", "ap-southeast", "ap-northeast"]
DEVICES  = ["ios", "ios", "ios", "android", "android", "web"]  # mostly mobile

def do_work(ms: float, jitter: float = 0.3):
    """Sleep to simulate latency with jitter."""
    actual = max(1, random.gauss(ms, ms * jitter))
    time.sleep(actual / 1000)
    return actual

def maybe_fail(fail_chance: float, error_msg: str):
    if random.random() < fail_chance:
        raise RuntimeError(error_msg)


# ═══════════════════════════════════════════════════════════════
#  MICROSERVICE SPAN TREES
#  Each function = one microservice call with its own children
# ═══════════════════════════════════════════════════════════════

def auth_service(user_id: str, device: str, is_drop: bool):
    """Auth Service: validate JWT, check account standing, MFA."""
    with tracer.start_as_current_span("auth-service: validate_request") as span:
        span.set_attribute("service.name",  "auth-service")
        span.set_attribute("user.id",       user_id)
        span.set_attribute("device.type",   device)

        with tracer.start_as_current_span("auth-service: verify_jwt"):
            do_work(5 if not is_drop else 15)  # slower under load

        with tracer.start_as_current_span("auth-service: redis.get [session]") as s:
            s.set_attribute("db.system", "redis")
            s.set_attribute("db.operation", "GET")
            # Cache miss is more likely during drop (cold starts)
            cache_miss = random.random() < (0.05 if not is_drop else 0.35)
            s.set_attribute("cache.hit", not cache_miss)
            do_work(2 if not cache_miss else 12)

            if cache_miss:
                with tracer.start_as_current_span("auth-service: postgres.query [users]") as db:
                    db.set_attribute("db.system", "postgresql")
                    db.set_attribute("db.statement", "SELECT * FROM users WHERE id = $1")
                    do_work(20 if not is_drop else 60)  # DB under pressure during drop
                    maybe_fail(0.001 if not is_drop else 0.02, "DB connection pool exhausted")

        with tracer.start_as_current_span("auth-service: check_account_standing"):
            do_work(3)
            # Nike bans bots/duplicate entries during drops
            if is_drop and random.random() < 0.08:
                raise RuntimeError("Account flagged: suspected bot activity")

        logger.debug(f"Auth OK user={user_id} device={device} cache_hit={not cache_miss}")


def product_service(sneaker_id: str, is_drop: bool):
    """Product Service: fetch sneaker details, images, sizing."""
    with tracer.start_as_current_span("product-service: get_product") as span:
        span.set_attribute("service.name",   "product-service")
        span.set_attribute("product.id",     sneaker_id)
        span.set_attribute("product.name",   DROP_SNEAKER["name"])

        with tracer.start_as_current_span("product-service: cdn.get [product_images]") as cdn:
            cdn.set_attribute("cdn.provider", "akamai")
            cdn.set_attribute("http.url",     f"https://cdn.nike.com/products/{sneaker_id}")
            do_work(8)

        with tracer.start_as_current_span("product-service: redis.get [product_cache]") as c:
            c.set_attribute("db.system",    "redis")
            c.set_attribute("cache.key",    f"product:{sneaker_id}")
            do_work(3)

        with tracer.start_as_current_span("product-service: inventory-service: get_stock") as inv:
            inv.set_attribute("service.name",  "inventory-service")
            inv.set_attribute("product.sku",   DROP_SNEAKER["sku"])
            with state.lock:
                inv.set_attribute("inventory.remaining", state.inventory)
            do_work(10 if not is_drop else 35)

        logger.debug(f"Product fetched sku={DROP_SNEAKER['sku']} inventory={state.inventory}")


def draw_service(user_id: str, size: str, device: str):
    """Draw Service: enter the SNKRS draw (raffle system)."""
    with tracer.start_as_current_span("draw-service: enter_draw") as span:
        span.set_attribute("service.name",  "draw-service")
        span.set_attribute("user.id",       user_id)
        span.set_attribute("product.sku",   DROP_SNEAKER["sku"])
        span.set_attribute("shoe.size",     size)
        span.set_attribute("device.type",   device)

        # Validate entry window
        with tracer.start_as_current_span("draw-service: validate_entry_window"):
            do_work(5)

        # Check for duplicate entry
        with tracer.start_as_current_span("draw-service: redis.get [entry_dedup]") as dedup:
            dedup.set_attribute("db.system",  "redis")
            dedup.set_attribute("db.operation", "SISMEMBER")
            do_work(4)
            if random.random() < 0.03:
                raise RuntimeError("Duplicate draw entry detected")

        # Write entry to queue
        with tracer.start_as_current_span("draw-service: kafka.produce [draw_entries]") as kafka:
            kafka.set_attribute("messaging.system",      "kafka")
            kafka.set_attribute("messaging.destination", "snkrs.draw.entries")
            do_work(15)
            with state.lock:
                state.queue_entries += 1
                queue_depth.add(1, {"product": DROP_SNEAKER["id"]})

        # Persist to DB
        with tracer.start_as_current_span("draw-service: dynamodb.put [draw_entries]") as ddb:
            ddb.set_attribute("db.system",    "dynamodb")
            ddb.set_attribute("db.table",     "snkrs_draw_entries")
            ddb.set_attribute("db.operation", "PutItem")
            do_work(20)
            maybe_fail(0.005, "DynamoDB write capacity exceeded")

        logger.info(f"Draw entry submitted user={user_id} size={size} queue={state.queue_entries}")


def inventory_service_reserve(user_id: str, size: str):
    """Inventory Service: attempt to reserve a pair (race condition heaven)."""
    with tracer.start_as_current_span("inventory-service: reserve_pair") as span:
        span.set_attribute("service.name", "inventory-service")
        span.set_attribute("user.id",      user_id)
        span.set_attribute("shoe.size",    size)

        with state.lock:
            remaining = state.inventory

        span.set_attribute("inventory.at_time_of_request", remaining)

        # Optimistic locking check
        with tracer.start_as_current_span("inventory-service: redis.decr [stock]") as r:
            r.set_attribute("db.system",    "redis")
            r.set_attribute("db.operation", "DECRBY")
            do_work(8)

            with state.lock:
                if state.inventory <= 0:
                    raise RuntimeError("SOLD OUT: No inventory remaining")
                state.inventory -= 1
                inventory_gauge.add(-1, {"product": DROP_SNEAKER["id"], "size": size})

        with tracer.start_as_current_span("inventory-service: postgres.update [inventory]") as db:
            db.set_attribute("db.system",    "postgresql")
            db.set_attribute("db.statement", "UPDATE inventory SET qty = qty - 1 WHERE sku = $1 AND size = $2 AND qty > 0")
            do_work(25)

        logger.info(f"Inventory reserved user={user_id} size={size} remaining={state.inventory}")


def payment_service(user_id: str, amount: float, is_drop: bool):
    """Payment Service: charge the card."""
    with tracer.start_as_current_span("payment-service: process_payment") as span:
        span.set_attribute("service.name",   "payment-service")
        span.set_attribute("user.id",        user_id)
        span.set_attribute("payment.amount", amount)
        span.set_attribute("payment.currency", "USD")

        payment_counter.add(1, {"product": DROP_SNEAKER["id"]})

        with tracer.start_as_current_span("payment-service: validate_payment_method"):
            do_work(10)
            maybe_fail(0.02, "Payment method validation failed")

        with tracer.start_as_current_span("payment-service: fraud.score_check") as fraud:
            fraud.set_attribute("service.name", "fraud-detection")
            do_work(30 if not is_drop else 80)  # fraud checks spike during drops
            score = random.uniform(0, 100)
            fraud.set_attribute("fraud.score", round(score, 2))
            if score > 85:
                raise RuntimeError(f"High fraud risk score: {score:.1f}")

        with tracer.start_as_current_span("payment-service: stripe.charge") as stripe:
            stripe.set_attribute("payment.provider",  "stripe")
            stripe.set_attribute("payment.method",    "card")
            do_work(200)  # external API call — always slow
            maybe_fail(0.03 if not is_drop else 0.08, "Stripe API timeout")

        with tracer.start_as_current_span("payment-service: postgres.insert [transactions]") as db:
            db.set_attribute("db.system",    "postgresql")
            db.set_attribute("db.statement", "INSERT INTO transactions ...")
            do_work(15)

        logger.info(f"Payment processed user={user_id} amount=${amount}")


def order_service(user_id: str, size: str, amount: float):
    """Order Service: create order, trigger fulfillment."""
    with tracer.start_as_current_span("order-service: create_order") as span:
        span.set_attribute("service.name",   "order-service")
        span.set_attribute("user.id",        user_id)
        span.set_attribute("product.sku",    DROP_SNEAKER["sku"])
        span.set_attribute("shoe.size",      size)
        span.set_attribute("order.amount",   amount)

        with tracer.start_as_current_span("order-service: postgres.insert [orders]") as db:
            db.set_attribute("db.system",    "postgresql")
            db.set_attribute("db.statement", "INSERT INTO orders ...")
            do_work(20)

        with tracer.start_as_current_span("order-service: kafka.produce [order_created]") as kafka:
            kafka.set_attribute("messaging.system",      "kafka")
            kafka.set_attribute("messaging.destination", "snkrs.orders.created")
            do_work(10)

        with tracer.start_as_current_span("order-service: notification-service: send_confirmation") as notif:
            notif.set_attribute("service.name",       "notification-service")
            notif.set_attribute("notification.type",  "order_confirmation")
            notif.set_attribute("notification.channel", "push+email")

            with tracer.start_as_current_span("notification-service: apns.push") as apns:
                apns.set_attribute("push.provider", "apns")
                do_work(50)
                maybe_fail(0.01, "APNS delivery failed")

            with tracer.start_as_current_span("notification-service: ses.send_email") as ses:
                ses.set_attribute("email.provider", "aws-ses")
                do_work(80)

        logger.info(f"Order created user={user_id} sku={DROP_SNEAKER['sku']} size={size} amount=${amount}")


def notification_sorry(user_id: str, reason: str):
    """Send 'sorry, you didn't win' notification."""
    with tracer.start_as_current_span("notification-service: send_loss_notification") as span:
        span.set_attribute("service.name",      "notification-service")
        span.set_attribute("user.id",           user_id)
        span.set_attribute("notification.type", "draw_loss")
        span.set_attribute("loss.reason",       reason)
        do_work(60)
        logger.info(f"Loss notification sent user={user_id} reason={reason}")


# ═══════════════════════════════════════════════════════════════
#  FULL REQUEST FLOWS (one per user journey type)
# ═══════════════════════════════════════════════════════════════

def flow_browse_product(user_id, region, device):
    """User opens SNKRS app and views the upcoming drop."""
    with tracer.start_as_current_span("GET /v1/products/{id}") as root:
        root.set_attribute("http.method",    "GET")
        root.set_attribute("http.route",     "/v1/products/{id}")
        root.set_attribute("user.id",        user_id)
        root.set_attribute("cloud.region",   region)
        root.set_attribute("device.type",    device)
        root.set_attribute("drop.phase",     state.phase)
        try:
            auth_service(user_id, device, is_drop=False)
            product_service(DROP_SNEAKER["id"], is_drop=False)
            root.set_attribute("http.status_code", 200)
            logger.info(f"Product browsed user={user_id} product={DROP_SNEAKER['name']}")
        except RuntimeError as e:
            root.set_attribute("http.status_code", 503)
            root.set_attribute("error", True)
            root.record_exception(e)
            logger.error(f"Browse failed: {e}")


def flow_enter_draw(user_id, region, device):
    """User taps 'Enter Draw' the moment the drop goes live."""
    size = random.choice(["7", "7.5", "8", "8.5", "9", "9.5", "10", "10.5", "11", "12", "13"])

    with tracer.start_as_current_span("POST /v1/draw/enter") as root:
        root.set_attribute("http.method",    "POST")
        root.set_attribute("http.route",     "/v1/draw/enter")
        root.set_attribute("user.id",        user_id)
        root.set_attribute("cloud.region",   region)
        root.set_attribute("device.type",    device)
        root.set_attribute("drop.phase",     state.phase)
        root.set_attribute("product.sku",    DROP_SNEAKER["sku"])
        root.set_attribute("shoe.size",      size)

        try:
            auth_service(user_id, device, is_drop=True)
            product_service(DROP_SNEAKER["id"], is_drop=True)
            draw_service(user_id, size, device)
            root.set_attribute("http.status_code", 201)
            req_counter.add(1, {"endpoint": "/v1/draw/enter", "status": "201", "region": region})
            logger.info(f"Draw entered user={user_id} size={size}")
        except RuntimeError as e:
            root.set_attribute("http.status_code", 429 if "bot" in str(e).lower() else 500)
            root.set_attribute("error", True)
            root.record_exception(e)
            error_counter.add(1, {"endpoint": "/v1/draw/enter", "error": str(e)[:50]})
            logger.error(f"Draw entry failed user={user_id}: {e}")


def flow_checkout(user_id, region, device):
    """
    User wins the draw → inventory reserve → payment → order.
    This is the most complex flow with the most failure points.
    """
    size   = random.choice(["8", "9", "10", "11"])
    amount = DROP_SNEAKER["price"] + random.uniform(0, 30)  # price + tax

    with tracer.start_as_current_span("POST /v1/checkout") as root:
        root.set_attribute("http.method",    "POST")
        root.set_attribute("http.route",     "/v1/checkout")
        root.set_attribute("user.id",        user_id)
        root.set_attribute("cloud.region",   region)
        root.set_attribute("device.type",    device)
        root.set_attribute("drop.phase",     state.phase)
        root.set_attribute("product.sku",    DROP_SNEAKER["sku"])
        root.set_attribute("order.amount",   round(amount, 2))

        try:
            auth_service(user_id, device, is_drop=True)
            inventory_service_reserve(user_id, size)
            payment_service(user_id, round(amount, 2), is_drop=True)
            order_service(user_id, size, round(amount, 2))
            root.set_attribute("http.status_code", 201)
            req_counter.add(1, {"endpoint": "/v1/checkout", "status": "201", "region": region})
            logger.info(f"Checkout SUCCESS user={user_id} amount=${amount:.2f}")

        except RuntimeError as e:
            msg = str(e)
            if "SOLD OUT" in msg:
                code = 410  # Gone
                notification_sorry(user_id, "sold_out")
            elif "Stripe" in msg or "payment" in msg.lower():
                code = 402
            elif "fraud" in msg.lower():
                code = 403
            else:
                code = 500

            root.set_attribute("http.status_code", code)
            root.set_attribute("error", True)
            root.record_exception(e)
            error_counter.add(1, {"endpoint": "/v1/checkout", "error": msg[:50]})
            logger.error(f"Checkout FAILED user={user_id} code={code}: {msg}")


def flow_account_check(user_id, region, device):
    """User checks their account / payment methods before the drop."""
    with tracer.start_as_current_span("GET /v1/account/profile") as root:
        root.set_attribute("http.method",  "GET")
        root.set_attribute("http.route",   "/v1/account/profile")
        root.set_attribute("user.id",      user_id)
        root.set_attribute("cloud.region", region)
        root.set_attribute("device.type",  device)
        try:
            auth_service(user_id, device, is_drop=False)
            do_work(20)
            root.set_attribute("http.status_code", 200)
        except RuntimeError as e:
            root.set_attribute("http.status_code", 401)
            root.set_attribute("error", True)


def flow_feed(user_id, region, device):
    """Homepage feed — just CDN-heavy, low DB."""
    with tracer.start_as_current_span("GET /v1/feed") as root:
        root.set_attribute("http.method",  "GET")
        root.set_attribute("http.route",   "/v1/feed")
        root.set_attribute("user.id",      user_id)
        root.set_attribute("cloud.region", region)
        root.set_attribute("device.type",  device)

        with tracer.start_as_current_span("feed-service: redis.get [feed_cache]") as c:
            c.set_attribute("db.system", "redis")
            do_work(4)

        with tracer.start_as_current_span("feed-service: cdn.fetch [images]") as cdn:
            cdn.set_attribute("cdn.provider", "akamai")
            do_work(30)

        root.set_attribute("http.status_code", 200)


# ═══════════════════════════════════════════════════════════════
#  PHASE RUNNERS
# ═══════════════════════════════════════════════════════════════

def make_user():
    return f"user_{random.randint(100000, 999999)}"

def make_session():
    region = random.choice(REGIONS)
    device = random.choice(DEVICES)
    return make_user(), region, device


def run_pre_drop():
    """Calm browsing traffic before the drop."""
    user_id, region, device = make_session()
    flow = random.choices(
        [flow_browse_product, flow_account_check, flow_feed],
        weights=[40, 30, 30]
    )[0]
    flow(user_id, region, device)
    time.sleep(random.uniform(0.5, 2.0))


def run_drop():
    """Absolute chaos — everyone hitting Enter Draw simultaneously."""
    user_id, region, device = make_session()
    flow = random.choices(
        [flow_enter_draw, flow_checkout, flow_browse_product],
        weights=[55, 35, 10]
    )[0]
    flow(user_id, region, device)
    time.sleep(random.uniform(0.0, 0.1))   # minimal sleep — hammering the system


def run_post_drop():
    """Cooldown — people checking orders, notifications going out."""
    user_id, region, device = make_session()
    flow = random.choices(
        [flow_account_check, flow_browse_product, flow_feed],
        weights=[50, 30, 20]
    )[0]
    flow(user_id, region, device)
    time.sleep(random.uniform(1.0, 3.0))


def run_workers(fn, n_workers, duration, phase_name):
    """Run fn() in n_workers threads for `duration` seconds."""
    state.phase = phase_name
    end = time.time() + duration
    counts = {"ok": 0, "err": 0}

    def worker():
        while time.time() < end:
            try:
                fn()
                counts["ok"] += 1
            except Exception as e:
                counts["err"] += 1

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(n_workers)]
    for t in threads: t.start()

    # Progress reporting
    start = time.time()
    while time.time() < end:
        elapsed = time.time() - start
        print(f"  [{phase_name}] {elapsed:.0f}s / {duration}s | "
              f"ok={counts['ok']} err={counts['err']} "
              f"inventory={state.inventory} queue={state.queue_entries}",
              end="\r")
        time.sleep(2)

    for t in threads: t.join(timeout=5)
    print(f"\n  [{phase_name}] DONE — ok={counts['ok']} err={counts['err']}")


# ═══════════════════════════════════════════════════════════════
#  MAIN — runs forever, cycling through drop phases
# ═══════════════════════════════════════════════════════════════

drop_number = 0
while True:
    drop_number += 1
    state.inventory     = random.randint(300, 700)
    state.queue_entries = 0

    print(f"\n{'='*60}")
    print(f"  DROP #{drop_number}: {DROP_SNEAKER['name']}")
    print(f"  Stock: {state.inventory} pairs | Price: ${DROP_SNEAKER['price']}")
    print(f"{'='*60}")

    print(f"\n[Phase 1] PRE-DROP — Users browsing & prepping ({PRE_DROP_DURATION}s)")
    run_workers(run_pre_drop, PRE_DROP_WORKERS, PRE_DROP_DURATION, "PRE_DROP")

    print(f"\n[Phase 2] DROP LIVE 🔥 — {DROP_WORKERS} users hammering simultaneously ({DROP_DURATION}s)")
    run_workers(run_drop, DROP_WORKERS, DROP_DURATION, "DROP_LIVE")

    print(f"\n[Phase 3] SOLD OUT — Cooldown & notifications ({POST_DROP_DURATION}s)")
    run_workers(run_post_drop, POST_DROP_WORKERS, POST_DROP_DURATION, "POST_DROP")

    print(f"\n  Drop #{drop_number} complete. Restarting in 10s...")
    time.sleep(10)