"""
Distributed trace tree generator — produces realistic span trees
matching the existing 8-service SNKRS architecture.

Each API call produces a full span tree with proper parent-child relationships,
realistic durations, service-specific attributes, and error injection.
"""

import random
import uuid
from datetime import datetime, timedelta

from inventory import StockTracker
from patterns import should_apply_incident


# ── Helpers ──────────────────────────────────────────────────────────────

def _trace_id() -> str:
    """Generate a 32-char hex trace ID."""
    return uuid.uuid4().hex

def _span_id() -> str:
    """Generate a 16-char hex span ID."""
    return uuid.uuid4().hex[:16]

def _duration_ns(base_ms: float, jitter: float = 0.3, rng: random.Random = None) -> int:
    """Generate a duration in nanoseconds with gaussian jitter."""
    r = rng or random
    actual_ms = max(0.1, r.gauss(base_ms, base_ms * jitter))
    return int(actual_ms * 1_000_000)

def _resource_attrs(service_name: str, region: str) -> dict:
    """Standard resource attributes for a service."""
    return {
        "service.name": service_name,
        "service.version": "4.2.1",
        "deployment.environment": "production",
        "cloud.provider": "aws",
        "cloud.region": region,
    }

def _make_span(trace_id: str, parent_span_id: str, span_name: str,
               service_name: str, span_kind: str, timestamp: datetime,
               duration_ns: int, status: str, region: str,
               span_attrs: dict = None, status_message: str = "") -> dict:
    """Create a single span dict matching the otel_traces schema."""
    return {
        "Timestamp": timestamp,
        "TraceId": trace_id,
        "SpanId": _span_id(),
        "ParentSpanId": parent_span_id,
        "TraceState": "",
        "SpanName": span_name,
        "SpanKind": span_kind,
        "ServiceName": service_name,
        "ResourceAttributes": _resource_attrs(service_name, region),
        "ScopeName": "snkrs-simulator",
        "ScopeVersion": "1.0.0",
        "SpanAttributes": span_attrs or {},
        "Duration": duration_ns,
        "StatusCode": status,
        "StatusMessage": status_message,
        "Events.Timestamp": [],
        "Events.Name": [],
        "Events.Attributes": [],
        "Links.TraceId": [],
        "Links.SpanId": [],
        "Links.TraceState": [],
        "Links.Attributes": [],
    }


# ── Span tree builders ──────────────────────────────────────────────────

def _auth_spans(trace_id: str, parent_id: str, ts: datetime,
                user: dict, is_drop: bool, region: str,
                incident: dict, progress: float,
                rng: random.Random) -> tuple[list, int, bool]:
    """Auth service span tree. Returns (spans, total_duration_ns, errored)."""
    spans = []
    errored = False
    total_ns = 0

    # Check incident
    inc_active = should_apply_incident(incident, "auth-service", "", region, progress)
    lat_mult = incident["latency_multiplier"] if inc_active else 1.0
    err_override = incident["error_rate_override"] if inc_active else None

    # Root: validate_request
    root_id = _span_id()
    child_offset = 0

    # verify_jwt
    dur = _duration_ns(5 if not is_drop else 15, rng=rng)
    dur = int(dur * lat_mult)
    spans.append(_make_span(trace_id, root_id, "auth-service: verify_jwt",
                            "auth-service", "INTERNAL", ts + timedelta(microseconds=child_offset // 1000),
                            dur, "OK", region, {"auth.method": "jwt"}))
    child_offset += dur
    total_ns += dur

    # redis.get [session]
    cache_miss = rng.random() < (0.05 if not is_drop else 0.35)
    redis_dur = _duration_ns(2 if not cache_miss else 12, rng=rng)
    redis_dur = int(redis_dur * lat_mult)
    redis_id = _span_id()
    spans.append(_make_span(trace_id, root_id, "auth-service: redis.get [session]",
                            "auth-service", "CLIENT", ts + timedelta(microseconds=child_offset // 1000),
                            redis_dur, "OK", region,
                            {"db.system": "redis", "db.operation": "GET", "cache.hit": str(not cache_miss).lower()}))

    if cache_miss:
        pg_dur = _duration_ns(20 if not is_drop else 60, rng=rng)
        pg_dur = int(pg_dur * lat_mult)
        spans.append(_make_span(trace_id, redis_id, "auth-service: postgres.query [users]",
                                "auth-service", "CLIENT",
                                ts + timedelta(microseconds=(child_offset + redis_dur // 4) // 1000),
                                pg_dur, "OK", region,
                                {"db.system": "postgresql", "db.statement": "SELECT * FROM users WHERE id = $1"}))

    child_offset += redis_dur
    total_ns += redis_dur

    # check_account_standing
    standing_dur = _duration_ns(3, rng=rng)
    standing_dur = int(standing_dur * lat_mult)
    standing_err = (err_override and rng.random() < err_override) or \
                   (is_drop and rng.random() < 0.08) or \
                   (user.get("is_bot") and rng.random() < 0.40)
    status = "ERROR" if standing_err else "OK"
    msg = "Account flagged: suspected bot activity" if standing_err else ""
    spans.append(_make_span(trace_id, root_id, "auth-service: check_account_standing",
                            "auth-service", "INTERNAL",
                            ts + timedelta(microseconds=child_offset // 1000),
                            standing_dur, status, region,
                            {"user.id": user["user_id"], "account.standing": "blocked" if standing_err else "active"},
                            status_message=msg))
    child_offset += standing_dur
    total_ns += standing_dur

    if standing_err:
        errored = True

    # Root span
    root_status = "ERROR" if errored else "OK"
    root_msg = msg if errored else ""
    spans.append(_make_span(trace_id, parent_id, "auth-service: validate_request",
                            "auth-service", "INTERNAL", ts, total_ns, root_status, region,
                            {"user.id": user["user_id"], "device.type": user["device"]},
                            status_message=root_msg))

    return spans, total_ns, errored


def _product_spans(trace_id: str, parent_id: str, ts: datetime,
                   product: dict, is_drop: bool, region: str,
                   stock_tracker: StockTracker,
                   incident: dict, progress: float,
                   rng: random.Random) -> tuple[list, int]:
    """Product service span tree."""
    spans = []
    total_ns = 0

    inc_active = should_apply_incident(incident, "product-service", "", region, progress)
    lat_mult = incident["latency_multiplier"] if inc_active else 1.0

    root_id = _span_id()

    # cdn.get
    cdn_dur = _duration_ns(8, rng=rng)
    cdn_dur = int(cdn_dur * lat_mult)
    spans.append(_make_span(trace_id, root_id, "product-service: cdn.get [product_images]",
                            "product-service", "CLIENT", ts, cdn_dur, "OK", region,
                            {"cdn.provider": "akamai", "http.url": f"https://cdn.nike.com/products/{product['id']}"}))
    total_ns += cdn_dur

    # redis.get
    redis_dur = _duration_ns(3, rng=rng)
    redis_dur = int(redis_dur * lat_mult)
    spans.append(_make_span(trace_id, root_id, "product-service: redis.get [product_cache]",
                            "product-service", "CLIENT",
                            ts + timedelta(microseconds=total_ns // 1000),
                            redis_dur, "OK", region,
                            {"db.system": "redis", "cache.key": f"product:{product['id']}"}))
    total_ns += redis_dur

    # inventory-service: get_stock
    inv_dur = _duration_ns(10 if not is_drop else 35, rng=rng)
    inv_dur = int(inv_dur * lat_mult)
    remaining = stock_tracker.get_total_stock(product["id"])
    spans.append(_make_span(trace_id, root_id, "product-service: inventory-service: get_stock",
                            "inventory-service", "INTERNAL",
                            ts + timedelta(microseconds=total_ns // 1000),
                            inv_dur, "OK", region,
                            {"product.sku": product["sku"], "inventory.remaining": str(remaining)}))
    total_ns += inv_dur

    # root
    spans.append(_make_span(trace_id, parent_id, "product-service: get_product",
                            "product-service", "INTERNAL", ts, total_ns, "OK", region,
                            {"product.id": product["id"], "product.name": product["name"]}))

    return spans, total_ns


def _draw_spans(trace_id: str, parent_id: str, ts: datetime,
                user: dict, product: dict, region: str,
                incident: dict, progress: float,
                rng: random.Random) -> tuple[list, int, bool]:
    """Draw service span tree."""
    spans = []
    total_ns = 0
    errored = False

    inc_active = should_apply_incident(incident, "draw-service", "POST /v1/draw/enter", region, progress)
    lat_mult = incident["latency_multiplier"] if inc_active else 1.0

    root_id = _span_id()

    # validate_entry_window
    dur = _duration_ns(5, rng=rng)
    dur = int(dur * lat_mult)
    spans.append(_make_span(trace_id, root_id, "draw-service: validate_entry_window",
                            "draw-service", "INTERNAL", ts, dur, "OK", region))
    total_ns += dur

    # redis dedup check
    dedup_dur = _duration_ns(4, rng=rng)
    dedup_dur = int(dedup_dur * lat_mult)
    dedup_fail = rng.random() < 0.03
    spans.append(_make_span(trace_id, root_id, "draw-service: redis.get [entry_dedup]",
                            "draw-service", "CLIENT",
                            ts + timedelta(microseconds=total_ns // 1000),
                            dedup_dur, "ERROR" if dedup_fail else "OK", region,
                            {"db.system": "redis", "db.operation": "SISMEMBER"},
                            status_message="Duplicate draw entry detected" if dedup_fail else ""))
    total_ns += dedup_dur

    if dedup_fail:
        errored = True
    else:
        # kafka.produce
        kafka_dur = _duration_ns(15, rng=rng)
        kafka_dur = int(kafka_dur * lat_mult)
        spans.append(_make_span(trace_id, root_id, "draw-service: kafka.produce [draw_entries]",
                                "draw-service", "PRODUCER",
                                ts + timedelta(microseconds=total_ns // 1000),
                                kafka_dur, "OK", region,
                                {"messaging.system": "kafka", "messaging.destination": "snkrs.draw.entries"}))
        total_ns += kafka_dur

        # dynamodb.put
        ddb_dur = _duration_ns(20, rng=rng)
        ddb_dur = int(ddb_dur * lat_mult)
        ddb_fail = rng.random() < 0.005
        spans.append(_make_span(trace_id, root_id, "draw-service: dynamodb.put [draw_entries]",
                                "draw-service", "CLIENT",
                                ts + timedelta(microseconds=total_ns // 1000),
                                ddb_dur, "ERROR" if ddb_fail else "OK", region,
                                {"db.system": "dynamodb", "db.table": "snkrs_draw_entries", "db.operation": "PutItem"},
                                status_message="DynamoDB write capacity exceeded" if ddb_fail else ""))
        total_ns += ddb_dur
        if ddb_fail:
            errored = True

    # root
    root_status = "ERROR" if errored else "OK"
    spans.append(_make_span(trace_id, parent_id, "draw-service: enter_draw",
                            "draw-service", "INTERNAL", ts, total_ns, root_status, region,
                            {"user.id": user["user_id"], "product.sku": product["sku"],
                             "shoe.size": user["shoe_size"], "device.type": user["device"]}))

    return spans, total_ns, errored


def _inventory_reserve_spans(trace_id: str, parent_id: str, ts: datetime,
                             user: dict, product: dict, region: str,
                             stock_tracker: StockTracker,
                             incident: dict, progress: float,
                             rng: random.Random) -> tuple[list, int, bool]:
    """Inventory service reserve span tree."""
    spans = []
    total_ns = 0
    errored = False

    inc_active = should_apply_incident(incident, "inventory-service", "POST /v1/checkout", region, progress)
    lat_mult = incident["latency_multiplier"] if inc_active else 1.0
    err_override = incident["error_rate_override"] if inc_active else None

    root_id = _span_id()
    size = user["shoe_size"]

    success, remaining = stock_tracker.try_reserve(product["id"], size)

    # redis.decr
    redis_dur = _duration_ns(8, rng=rng)
    redis_dur = int(redis_dur * lat_mult)
    redis_status = "OK" if success else "ERROR"
    spans.append(_make_span(trace_id, root_id, "inventory-service: redis.decr [stock]",
                            "inventory-service", "CLIENT", ts, redis_dur, redis_status, region,
                            {"db.system": "redis", "db.operation": "DECRBY",
                             "inventory.remaining": str(remaining)},
                            status_message="" if success else "SOLD OUT: No inventory remaining"))
    total_ns += redis_dur

    if not success:
        errored = True
    elif err_override and rng.random() < err_override:
        errored = True
    else:
        # postgres.update
        pg_dur = _duration_ns(25, rng=rng)
        pg_dur = int(pg_dur * lat_mult)
        spans.append(_make_span(trace_id, root_id, "inventory-service: postgres.update [inventory]",
                                "inventory-service", "CLIENT",
                                ts + timedelta(microseconds=total_ns // 1000),
                                pg_dur, "OK", region,
                                {"db.system": "postgresql",
                                 "db.statement": "UPDATE inventory SET qty = qty - 1 WHERE sku = $1 AND size = $2 AND qty > 0"}))
        total_ns += pg_dur

    root_status = "ERROR" if errored else "OK"
    root_msg = "SOLD OUT: No inventory remaining" if (not success) else \
               ((incident.get("error_message", "") if incident else "") if errored else "")
    spans.append(_make_span(trace_id, parent_id, "inventory-service: reserve_pair",
                            "inventory-service", "INTERNAL", ts, total_ns, root_status, region,
                            {"user.id": user["user_id"], "shoe.size": size,
                             "product.sku": product["sku"], "inventory.remaining": str(remaining)},
                            status_message=root_msg))

    return spans, total_ns, errored


def _payment_spans(trace_id: str, parent_id: str, ts: datetime,
                   user: dict, amount: float, region: str,
                   incident: dict, progress: float,
                   rng: random.Random) -> tuple[list, int, bool]:
    """Payment service span tree."""
    spans = []
    total_ns = 0
    errored = False

    inc_active = should_apply_incident(incident, "payment-service", "POST /v1/checkout", region, progress)
    lat_mult = incident["latency_multiplier"] if inc_active else 1.0
    err_override = incident["error_rate_override"] if inc_active else None

    root_id = _span_id()

    # validate_payment_method
    val_dur = _duration_ns(10, rng=rng)
    val_dur = int(val_dur * lat_mult)
    val_fail = rng.random() < 0.02 or (user["behavior_type"] == "new_user" and rng.random() < 0.08)
    spans.append(_make_span(trace_id, root_id, "payment-service: validate_payment_method",
                            "payment-service", "INTERNAL", ts, val_dur,
                            "ERROR" if val_fail else "OK", region,
                            {"payment.method": user["payment_method"]},
                            status_message="Payment method validation failed" if val_fail else ""))
    total_ns += val_dur

    if val_fail:
        errored = True
    else:
        # fraud.score_check
        fraud_dur = _duration_ns(30 if not inc_active else 80, rng=rng)
        fraud_dur = int(fraud_dur * lat_mult)
        fraud_score = user["fraud_risk"] * 100 * rng.uniform(0.5, 1.5)
        fraud_score = min(100, fraud_score)
        fraud_blocked = fraud_score > 85
        spans.append(_make_span(trace_id, root_id, "payment-service: fraud.score_check",
                                "payment-service", "INTERNAL",
                                ts + timedelta(microseconds=total_ns // 1000),
                                fraud_dur, "ERROR" if fraud_blocked else "OK", region,
                                {"fraud.score": f"{fraud_score:.2f}", "fraud.blocked": str(fraud_blocked).lower()},
                                status_message=f"High fraud risk score: {fraud_score:.1f}" if fraud_blocked else ""))
        total_ns += fraud_dur

        if fraud_blocked:
            errored = True
        else:
            # stripe.charge
            stripe_dur = _duration_ns(200, rng=rng)
            stripe_dur = int(stripe_dur * lat_mult)
            stripe_fail = (err_override and rng.random() < err_override) or rng.random() < 0.03
            spans.append(_make_span(trace_id, root_id, "payment-service: stripe.charge",
                                    "payment-service", "CLIENT",
                                    ts + timedelta(microseconds=total_ns // 1000),
                                    stripe_dur, "ERROR" if stripe_fail else "OK", region,
                                    {"payment.provider": "stripe", "payment.method": user["payment_method"],
                                     "payment.amount": f"{amount:.2f}", "payment.currency": "USD"},
                                    status_message=(incident.get("error_message", "Stripe API timeout") if incident else "Stripe API timeout") if stripe_fail else ""))
            total_ns += stripe_dur

            if stripe_fail:
                errored = True
            else:
                # postgres.insert [transactions]
                pg_dur = _duration_ns(15, rng=rng)
                pg_dur = int(pg_dur * lat_mult)
                spans.append(_make_span(trace_id, root_id, "payment-service: postgres.insert [transactions]",
                                        "payment-service", "CLIENT",
                                        ts + timedelta(microseconds=total_ns // 1000),
                                        pg_dur, "OK", region,
                                        {"db.system": "postgresql",
                                         "db.statement": "INSERT INTO transactions (user_id, amount, status) VALUES ($1, $2, $3)"}))
                total_ns += pg_dur

    root_status = "ERROR" if errored else "OK"
    spans.append(_make_span(trace_id, parent_id, "payment-service: process_payment",
                            "payment-service", "INTERNAL", ts, total_ns, root_status, region,
                            {"user.id": user["user_id"], "payment.amount": f"{amount:.2f}",
                             "payment.currency": "USD", "payment.method": user["payment_method"]},
                            status_message=spans[-1]["StatusMessage"] if errored else ""))

    return spans, total_ns, errored


def _order_spans(trace_id: str, parent_id: str, ts: datetime,
                 user: dict, product: dict, amount: float, region: str,
                 rng: random.Random) -> tuple[list, int]:
    """Order service + notification span tree."""
    spans = []
    total_ns = 0

    root_id = _span_id()

    # postgres.insert [orders]
    pg_dur = _duration_ns(20, rng=rng)
    spans.append(_make_span(trace_id, root_id, "order-service: postgres.insert [orders]",
                            "order-service", "CLIENT", ts, pg_dur, "OK", region,
                            {"db.system": "postgresql", "db.statement": "INSERT INTO orders (user_id, sku, size, amount) VALUES ($1, $2, $3, $4)"}))
    total_ns += pg_dur

    # kafka.produce
    kafka_dur = _duration_ns(10, rng=rng)
    spans.append(_make_span(trace_id, root_id, "order-service: kafka.produce [order_created]",
                            "order-service", "PRODUCER",
                            ts + timedelta(microseconds=total_ns // 1000),
                            kafka_dur, "OK", region,
                            {"messaging.system": "kafka", "messaging.destination": "snkrs.orders.created"}))
    total_ns += kafka_dur

    # notification-service
    notif_id = _span_id()

    # apns.push
    apns_dur = _duration_ns(50, rng=rng)
    apns_fail = rng.random() < 0.01
    spans.append(_make_span(trace_id, notif_id, "notification-service: apns.push",
                            "notification-service", "CLIENT",
                            ts + timedelta(microseconds=total_ns // 1000),
                            apns_dur, "ERROR" if apns_fail else "OK", region,
                            {"push.provider": "apns"},
                            status_message="APNS delivery failed" if apns_fail else ""))

    # ses.send_email
    ses_dur = _duration_ns(80, rng=rng)
    spans.append(_make_span(trace_id, notif_id, "notification-service: ses.send_email",
                            "notification-service", "CLIENT",
                            ts + timedelta(microseconds=(total_ns + apns_dur) // 1000),
                            ses_dur, "OK", region,
                            {"email.provider": "aws-ses"}))

    notif_dur = apns_dur + ses_dur
    spans.append(_make_span(trace_id, root_id, "order-service: notification-service: send_confirmation",
                            "notification-service", "INTERNAL",
                            ts + timedelta(microseconds=total_ns // 1000),
                            notif_dur, "OK", region,
                            {"notification.type": "order_confirmation", "notification.channel": "push+email"}))
    total_ns += notif_dur

    # root
    spans.append(_make_span(trace_id, parent_id, "order-service: create_order",
                            "order-service", "INTERNAL", ts, total_ns, "OK", region,
                            {"user.id": user["user_id"], "product.sku": product["sku"],
                             "shoe.size": user["shoe_size"], "order.amount": f"{amount:.2f}"}))

    return spans, total_ns


# ── Full flow generators ────────────────────────────────────────────────

def generate_browse_product(user: dict, product: dict, ts: datetime,
                            stock_tracker: StockTracker,
                            incident: dict = None, progress: float = 0.5,
                            rng: random.Random = None) -> list[dict]:
    """GET /v1/products/{id} — browse a product."""
    r = rng or random
    trace = _trace_id()
    region = user["region"]
    root_id = _span_id()
    spans = []
    offset_ns = 0

    # auth
    auth_s, auth_dur, auth_err = _auth_spans(
        trace, root_id, ts, user, False, region, incident, progress, r)
    spans.extend(auth_s)
    offset_ns += auth_dur

    if not auth_err:
        prod_s, prod_dur = _product_spans(
            trace, root_id, ts + timedelta(microseconds=offset_ns // 1000),
            product, False, region, stock_tracker, incident, progress, r)
        spans.extend(prod_s)
        offset_ns += prod_dur

    status = "ERROR" if auth_err else "OK"
    http_code = "503" if auth_err else "200"
    spans.append(_make_span(trace, "", "GET /v1/products/{id}",
                            "api-gateway", "SERVER", ts, offset_ns, status, region,
                            {"http.method": "GET", "http.route": "/v1/products/{id}",
                             "http.status_code": http_code, "user.id": user["user_id"],
                             "cloud.region": region, "device.type": user["device"],
                             "product.id": product["id"]},
                            status_message="" if not auth_err else "Auth failed"))
    # Fix: set root span's SpanId to root_id
    spans[-1]["SpanId"] = root_id

    return spans


def generate_enter_draw(user: dict, product: dict, ts: datetime,
                        stock_tracker: StockTracker,
                        incident: dict = None, progress: float = 0.5,
                        rng: random.Random = None) -> list[dict]:
    """POST /v1/draw/enter — enter the SNKRS draw."""
    r = rng or random
    trace = _trace_id()
    region = user["region"]
    root_id = _span_id()
    spans = []
    offset_ns = 0

    # auth
    auth_s, auth_dur, auth_err = _auth_spans(
        trace, root_id, ts, user, True, region, incident, progress, r)
    spans.extend(auth_s)
    offset_ns += auth_dur

    if not auth_err:
        prod_s, prod_dur = _product_spans(
            trace, root_id, ts + timedelta(microseconds=offset_ns // 1000),
            product, True, region, stock_tracker, incident, progress, r)
        spans.extend(prod_s)
        offset_ns += prod_dur

        draw_s, draw_dur, draw_err = _draw_spans(
            trace, root_id, ts + timedelta(microseconds=offset_ns // 1000),
            user, product, region, incident, progress, r)
        spans.extend(draw_s)
        offset_ns += draw_dur

    any_err = auth_err
    status = "ERROR" if any_err else "OK"
    http_code = "429" if (auth_err and user.get("is_bot")) else "500" if any_err else "201"
    spans.append(_make_span(trace, "", "POST /v1/draw/enter",
                            "api-gateway", "SERVER", ts, offset_ns, status, region,
                            {"http.method": "POST", "http.route": "/v1/draw/enter",
                             "http.status_code": http_code, "user.id": user["user_id"],
                             "cloud.region": region, "device.type": user["device"],
                             "product.sku": product["sku"], "shoe.size": user["shoe_size"]}))
    spans[-1]["SpanId"] = root_id

    return spans


def generate_checkout(user: dict, product: dict, ts: datetime,
                      stock_tracker: StockTracker,
                      incident: dict = None, progress: float = 0.5,
                      rng: random.Random = None) -> list[dict]:
    """POST /v1/checkout — full checkout flow (most complex, 8-15 spans)."""
    r = rng or random
    trace = _trace_id()
    region = user["region"]
    root_id = _span_id()
    spans = []
    offset_ns = 0
    amount = product["price"] + r.uniform(0, 30)

    # auth
    auth_s, auth_dur, auth_err = _auth_spans(
        trace, root_id, ts, user, True, region, incident, progress, r)
    spans.extend(auth_s)
    offset_ns += auth_dur

    checkout_err = auth_err
    err_msg = ""

    if not checkout_err:
        # inventory reserve
        inv_s, inv_dur, inv_err = _inventory_reserve_spans(
            trace, root_id, ts + timedelta(microseconds=offset_ns // 1000),
            user, product, region, stock_tracker, incident, progress, r)
        spans.extend(inv_s)
        offset_ns += inv_dur

        if inv_err:
            checkout_err = True
            err_msg = "Inventory reserve failed"
        else:
            # payment
            pay_s, pay_dur, pay_err = _payment_spans(
                trace, root_id, ts + timedelta(microseconds=offset_ns // 1000),
                user, round(amount, 2), region, incident, progress, r)
            spans.extend(pay_s)
            offset_ns += pay_dur

            if pay_err:
                checkout_err = True
                err_msg = "Payment failed"
            else:
                # order
                ord_s, ord_dur = _order_spans(
                    trace, root_id, ts + timedelta(microseconds=offset_ns // 1000),
                    user, product, round(amount, 2), region, r)
                spans.extend(ord_s)
                offset_ns += ord_dur

    status = "ERROR" if checkout_err else "OK"
    if checkout_err:
        if "SOLD OUT" in err_msg or any("SOLD OUT" in s.get("StatusMessage", "") for s in spans):
            http_code = "410"
        elif "fraud" in err_msg.lower() or any("fraud" in s.get("StatusMessage", "").lower() for s in spans):
            http_code = "403"
        elif "payment" in err_msg.lower() or "Stripe" in err_msg:
            http_code = "402"
        else:
            http_code = "500"
    else:
        http_code = "201"

    spans.append(_make_span(trace, "", "POST /v1/checkout",
                            "api-gateway", "SERVER", ts, offset_ns, status, region,
                            {"http.method": "POST", "http.route": "/v1/checkout",
                             "http.status_code": http_code, "user.id": user["user_id"],
                             "cloud.region": region, "device.type": user["device"],
                             "product.sku": product["sku"], "shoe.size": user["shoe_size"],
                             "order.amount": f"{round(amount, 2):.2f}"}))
    spans[-1]["SpanId"] = root_id

    return spans


def generate_account_check(user: dict, ts: datetime,
                           incident: dict = None, progress: float = 0.5,
                           rng: random.Random = None) -> list[dict]:
    """GET /v1/account/profile — account check."""
    r = rng or random
    trace = _trace_id()
    region = user["region"]
    root_id = _span_id()
    spans = []

    auth_s, auth_dur, auth_err = _auth_spans(
        trace, root_id, ts, user, False, region, incident, progress, r)
    spans.extend(auth_s)

    # profile fetch
    if not auth_err:
        profile_dur = _duration_ns(20, rng=r)
        spans.append(_make_span(trace, root_id, "auth-service: postgres.query [profile]",
                                "auth-service", "CLIENT",
                                ts + timedelta(microseconds=auth_dur // 1000),
                                profile_dur, "OK", region,
                                {"db.system": "postgresql", "db.statement": "SELECT * FROM user_profiles WHERE user_id = $1"}))
        total_dur = auth_dur + profile_dur
    else:
        total_dur = auth_dur

    status = "ERROR" if auth_err else "OK"
    http_code = "401" if auth_err else "200"
    spans.append(_make_span(trace, "", "GET /v1/account/profile",
                            "api-gateway", "SERVER", ts, total_dur, status, region,
                            {"http.method": "GET", "http.route": "/v1/account/profile",
                             "http.status_code": http_code, "user.id": user["user_id"],
                             "cloud.region": region, "device.type": user["device"]}))
    spans[-1]["SpanId"] = root_id

    return spans


def generate_feed(user: dict, ts: datetime,
                  incident: dict = None, progress: float = 0.5,
                  rng: random.Random = None) -> list[dict]:
    """GET /v1/feed — homepage feed."""
    r = rng or random
    trace = _trace_id()
    region = user["region"]
    root_id = _span_id()
    spans = []
    total_ns = 0

    inc_active = should_apply_incident(incident, "product-service", "GET /v1/feed", region, progress)
    lat_mult = incident["latency_multiplier"] if inc_active else 1.0

    # redis cache
    redis_dur = _duration_ns(4, rng=r)
    redis_dur = int(redis_dur * lat_mult)
    spans.append(_make_span(trace, root_id, "feed-service: redis.get [feed_cache]",
                            "feed-service", "CLIENT", ts, redis_dur, "OK", region,
                            {"db.system": "redis"}))
    total_ns += redis_dur

    # cdn fetch
    cdn_dur = _duration_ns(30, rng=r)
    cdn_dur = int(cdn_dur * lat_mult)
    spans.append(_make_span(trace, root_id, "feed-service: cdn.fetch [images]",
                            "feed-service", "CLIENT",
                            ts + timedelta(microseconds=total_ns // 1000),
                            cdn_dur, "OK", region,
                            {"cdn.provider": "akamai"}))
    total_ns += cdn_dur

    spans.append(_make_span(trace, "", "GET /v1/feed",
                            "api-gateway", "SERVER", ts, total_ns, "OK", region,
                            {"http.method": "GET", "http.route": "/v1/feed",
                             "http.status_code": "200", "user.id": user["user_id"],
                             "cloud.region": region, "device.type": user["device"]}))
    spans[-1]["SpanId"] = root_id

    return spans


# Flow dispatch map
FLOW_GENERATORS = {
    "GET /v1/products/{id}": generate_browse_product,
    "POST /v1/draw/enter": generate_enter_draw,
    "POST /v1/checkout": generate_checkout,
    "GET /v1/account/profile": generate_account_check,
    "GET /v1/feed": generate_feed,
}
