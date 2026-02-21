"""
Correlated log record generator — produces log records linked to span trees
via TraceId/SpanId.
"""

import random
from datetime import datetime, timedelta

# Severity mapping matching ClickHouse schema
SEVERITY_MAP = {
    "TRACE": 1,
    "DEBUG": 5,
    "INFO": 9,
    "WARN": 13,
    "ERROR": 17,
    "FATAL": 21,
}

# Service-specific log message templates
LOG_TEMPLATES = {
    "auth-service": {
        "DEBUG": [
            "JWT token validated for user {user_id}, expires in {ttl}s",
            "Session cache lookup key=session:{user_id} hit={cache_hit}",
            "Account standing check passed for user {user_id}",
            "Redis connection pool: active={active} idle={idle}",
        ],
        "INFO": [
            "Authentication successful user={user_id} device={device} method=jwt",
            "Session refreshed for user {user_id}, new TTL=3600s",
            "User {user_id} account verified, standing=active loyalty={loyalty}",
        ],
        "WARN": [
            "Elevated bot score detected user={user_id} score={fraud_score:.1f} threshold=70",
            "Session cache miss rate elevated: {miss_rate:.1f}% (threshold: 20%)",
            "Rate limit approaching for user {user_id}: {rate}/100 requests in window",
        ],
        "ERROR": [
            "Account flagged: suspected bot activity user={user_id} fingerprint_mismatch=true",
            "JWT validation failed: token expired user={user_id}",
            "Database connection pool exhausted: {active}/{max_conn} connections in use",
            "Account suspended user={user_id} reason=terms_violation",
        ],
    },
    "product-service": {
        "DEBUG": [
            "Product cache hit sku={sku} ttl_remaining={ttl}s",
            "CDN fetch completed url=https://cdn.nike.com/products/{product_id} latency={latency}ms",
            "Inventory query result sku={sku} remaining={stock}",
        ],
        "INFO": [
            "Product details served sku={sku} price=${price} stock={stock}",
            "Product image CDN cache warm for {product_id}",
        ],
        "WARN": [
            "CDN response slow for {product_id}: {latency}ms (threshold: 500ms)",
            "Product cache miss rate elevated: {miss_rate:.1f}%",
        ],
        "ERROR": [
            "CDN timeout: upstream provider returned 504 after 30s product={product_id}",
            "Product not found: {sku}",
        ],
    },
    "inventory-service": {
        "DEBUG": [
            "Stock check sku={sku} size={size} remaining={stock}",
            "Redis DECRBY stock:{sku}:{size} result={stock}",
        ],
        "INFO": [
            "Inventory reserved user={user_id} sku={sku} size={size} remaining={stock}",
            "Stock level update: {sku} size={size} qty={stock}",
        ],
        "WARN": [
            "Low stock alert sku={sku} size={size} remaining={stock} threshold=10",
            "Inventory contention detected: {retries} optimistic lock retries for {sku}",
        ],
        "ERROR": [
            "SOLD OUT: No inventory remaining sku={sku} size={size}",
            "Inventory consistency error: stock negative after concurrent decrement sku={sku} size={size} qty={stock}",
            "Reservation timeout after {timeout}ms for sku={sku}",
        ],
    },
    "payment-service": {
        "DEBUG": [
            "Payment method validation: user={user_id} method={payment_method}",
            "Fraud score computed user={user_id} score={fraud_score:.2f}",
            "Stripe charge initiated amount=${amount:.2f} currency=USD",
        ],
        "INFO": [
            "Payment processed user={user_id} amount=${amount:.2f} method={payment_method} provider=stripe",
            "Transaction recorded tx_id={tx_id} user={user_id} status=success",
        ],
        "WARN": [
            "Fraud score elevated user={user_id} score={fraud_score:.1f} (threshold=85)",
            "Payment retry attempt {attempt}/3 for user {user_id}",
            "Stripe API latency elevated: {latency}ms (p99 threshold: 500ms)",
        ],
        "ERROR": [
            "Payment method validation failed user={user_id} method={payment_method}",
            "High fraud risk score: {fraud_score:.1f} user={user_id} — payment blocked",
            "Stripe API timeout after 30s user={user_id} amount=${amount:.2f}",
            "Connection pool exhausted: max connections reached ({active}/{max_conn})",
        ],
    },
    "draw-service": {
        "DEBUG": [
            "Draw entry window validated: {window_start} to {window_end}",
            "Dedup check user={user_id} sku={sku} result=unique",
            "Kafka produce to snkrs.draw.entries partition={partition}",
        ],
        "INFO": [
            "Draw entry submitted user={user_id} sku={sku} size={size} queue_position={queue_pos}",
            "Draw entry persisted to DynamoDB user={user_id}",
        ],
        "WARN": [
            "Draw queue depth elevated: {queue_depth} entries (threshold: 5000)",
            "DynamoDB write capacity approaching limit: {consumed}/{provisioned} WCU",
        ],
        "ERROR": [
            "Duplicate draw entry detected user={user_id} sku={sku}",
            "DynamoDB write capacity exceeded — throttled",
            "Draw entry window closed for sku={sku}",
        ],
    },
    "order-service": {
        "DEBUG": [
            "Order record created user={user_id} sku={sku} size={size}",
            "Kafka produce to snkrs.orders.created",
        ],
        "INFO": [
            "Order created user={user_id} sku={sku} size={size} amount=${amount:.2f} order_id={order_id}",
            "Order fulfillment initiated order_id={order_id}",
        ],
        "WARN": [
            "Order processing delayed: upstream payment confirmation pending for {user_id}",
        ],
        "ERROR": [
            "Order creation failed: database constraint violation user={user_id}",
        ],
    },
    "notification-service": {
        "DEBUG": [
            "APNS push payload constructed for user={user_id} type={notif_type}",
            "SES email queued to={email} template=order_confirmation",
        ],
        "INFO": [
            "Notification sent user={user_id} channel=push+email type={notif_type}",
            "Push notification delivered via APNS user={user_id}",
        ],
        "WARN": [
            "APNS delivery delayed: retry in 5s user={user_id}",
            "SES sending rate approaching limit: {rate}/14 per second",
        ],
        "ERROR": [
            "APNS delivery failed user={user_id}: device token expired",
            "SES bounce: hard bounce for {email}",
        ],
    },
    "api-gateway": {
        "DEBUG": [
            "Request routed {method} {route} → {service}",
            "Response serialized in {latency}ms status={status_code}",
        ],
        "INFO": [
            "{method} {route} completed status={status_code} latency={latency}ms user={user_id}",
        ],
        "WARN": [
            "Request latency exceeded SLO: {method} {route} took {latency}ms (SLO: 1000ms)",
            "Rate limit warning for user {user_id}: {rate}/100 in 60s window",
        ],
        "ERROR": [
            "Request failed {method} {route} status={status_code} error={error_msg}",
            "Upstream service timeout: {service} did not respond within 30s",
        ],
    },
}


def generate_logs_for_spans(spans: list[dict], user: dict, product: dict = None,
                            rng: random.Random = None) -> list[dict]:
    """Generate correlated log records for a span tree.

    Distribution: ~60% DEBUG, 25% INFO, 10% WARN, 5% ERROR.
    Error logs correlate with failed spans.
    """
    r = rng or random
    logs = []

    for span in spans:
        service = span["ServiceName"]
        trace_id = span["TraceId"]
        span_id = span["SpanId"]
        is_error = span["StatusCode"] == "ERROR"
        ts = span["Timestamp"]
        attrs = span.get("SpanAttributes", {})

        templates = LOG_TEMPLATES.get(service)
        if not templates:
            continue

        # Always generate at least one log per span
        # Error spans always get an ERROR log
        if is_error and "ERROR" in templates:
            severity = "ERROR"
            template = r.choice(templates["ERROR"])
            logs.append(_make_log(trace_id, span_id, severity, template,
                                  ts, service, span, user, product, r))

        # Generate DEBUG/INFO logs based on distribution
        severity_roll = r.random()
        if severity_roll < 0.60:
            severity = "DEBUG"
        elif severity_roll < 0.85:
            severity = "INFO"
        elif severity_roll < 0.95:
            severity = "WARN"
        else:
            severity = "ERROR" if not is_error else "WARN"  # avoid double error

        if severity in templates:
            template = r.choice(templates[severity])
            logs.append(_make_log(trace_id, span_id, severity, template,
                                  ts, service, span, user, product, r))

    return logs


def _make_log(trace_id: str, span_id: str, severity: str, template: str,
              ts: datetime, service: str, span: dict, user: dict,
              product: dict = None, rng: random.Random = None) -> dict:
    """Create a single log record matching the otel_logs schema."""
    r = rng or random
    attrs = span.get("SpanAttributes", {})

    # Build template context
    ctx = {
        "user_id": user["user_id"],
        "device": user["device"],
        "loyalty": user["loyalty_tier"],
        "payment_method": user["payment_method"],
        "fraud_score": user["fraud_risk"] * 100 * r.uniform(0.5, 1.5),
        "cache_hit": attrs.get("cache.hit", "true"),
        "ttl": r.randint(300, 3600),
        "active": r.randint(10, 50),
        "idle": r.randint(1, 10),
        "max_conn": 50,
        "miss_rate": r.uniform(1, 40),
        "rate": r.randint(10, 95),
        "latency": r.randint(5, 2000),
        "timeout": r.randint(1000, 30000),
        "retries": r.randint(1, 5),
        "attempt": r.randint(1, 3),
        "queue_depth": r.randint(100, 10000),
        "queue_pos": r.randint(1, 5000),
        "partition": r.randint(0, 11),
        "consumed": r.randint(100, 400),
        "provisioned": 500,
        "tx_id": f"tx_{r.randint(100000, 999999)}",
        "order_id": f"ord_{r.randint(100000, 999999)}",
        "window_start": "10:00:00",
        "window_end": "10:15:00",
        "method": attrs.get("http.method", "GET"),
        "route": attrs.get("http.route", "/unknown"),
        "status_code": attrs.get("http.status_code", "200"),
        "service": service,
        "error_msg": span.get("StatusMessage", "unknown error"),
        "notif_type": attrs.get("notification.type", "order_confirmation"),
        "email": user.get("email", "user@example.com"),
    }

    if product:
        ctx.update({
            "sku": product["sku"],
            "product_id": product["id"],
            "price": product["price"],
            "size": user["shoe_size"],
            "stock": r.randint(0, product["base_stock"]),
            "amount": product["price"] + r.uniform(0, 30),
        })
    else:
        ctx.update({
            "sku": "unknown",
            "product_id": "unknown",
            "price": 0,
            "size": user["shoe_size"],
            "stock": 0,
            "amount": 0,
        })

    try:
        body = template.format(**ctx)
    except (KeyError, ValueError):
        body = template  # fallback if template has unknown keys

    # Offset log timestamp slightly within the span
    log_offset_ns = r.randint(0, max(1, span["Duration"] // 2))
    log_ts = ts + timedelta(microseconds=log_offset_ns // 1000)

    resource_attrs = {
        "service.name": service,
        "service.version": "4.2.1",
        "deployment.environment": "production",
        "cloud.provider": "aws",
        "cloud.region": user["region"],
    }

    return {
        "Timestamp": log_ts,
        "TraceId": trace_id,
        "SpanId": span_id,
        "SeverityNumber": SEVERITY_MAP[severity],
        "SeverityText": severity,
        "Body": body,
        "ServiceName": service,
        "ResourceAttributes": resource_attrs,
        "LogAttributes": {
            "user.id": user["user_id"],
            "log.source": f"{service}.handler",
        },
    }
