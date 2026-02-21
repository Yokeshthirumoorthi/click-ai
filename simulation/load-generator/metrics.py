"""
Time-series metric data point generator — produces ClickHouse-ready metric rows
at configurable intervals.
"""

import random
from datetime import datetime, timedelta

METRIC_DEFS = [
    {"name": "snkrs.requests",     "description": "Total API requests",      "unit": "1",  "type": "Sum"},
    {"name": "snkrs.inventory",    "description": "Remaining inventory",     "unit": "1",  "type": "Gauge"},
    {"name": "snkrs.queue_depth",  "description": "Draw queue depth",        "unit": "1",  "type": "Gauge"},
    {"name": "snkrs.errors",       "description": "Total errors",            "unit": "1",  "type": "Sum"},
    {"name": "snkrs.payments",     "description": "Payment attempts",        "unit": "1",  "type": "Sum"},
    {"name": "snkrs.latency_p99",  "description": "P99 latency per service", "unit": "ms", "type": "Gauge"},
    {"name": "snkrs.active_users", "description": "Active users",            "unit": "1",  "type": "Gauge"},
]

ENDPOINTS = ["/v1/products/{id}", "/v1/draw/enter", "/v1/checkout", "/v1/account/profile", "/v1/feed"]
SERVICES = ["api-gateway", "auth-service", "product-service", "draw-service",
            "inventory-service", "payment-service", "order-service", "notification-service"]
REGIONS = ["us-east", "us-west", "eu-west", "ap-southeast", "ap-northeast"]
DEVICES = ["ios", "android", "web"]
STATUSES = ["200", "201", "400", "401", "402", "403", "404", "410", "429", "500", "503"]
ERROR_TYPES = ["timeout", "connection_pool", "validation", "auth", "inventory", "payment", "fraud"]
PAYMENT_STATUSES = ["success", "failed", "fraud_blocked"]

RESOURCE_ATTRS = {
    "service.name": "snkrs-platform",
    "service.version": "4.2.1",
    "deployment.environment": "production",
    "cloud.provider": "aws",
    "cloud.region": "us-east-1",
}


def generate_metrics_for_interval(ts: datetime, phase: str, drop_product: dict,
                                  stock_remaining: int, queue_depth: int,
                                  active_users: int, span_stats: dict = None,
                                  incident: dict = None,
                                  rng: random.Random = None) -> list[dict]:
    """Generate metric data points for a single 10s interval.

    Args:
        ts: Timestamp for this interval
        phase: "pre_drop", "drop_live", or "post_drop"
        drop_product: Current drop product dict
        stock_remaining: Total remaining inventory
        queue_depth: Current draw queue depth
        active_users: Number of active users
        span_stats: Optional dict of counts from span generation
        incident: Optional active incident
        rng: Random instance
    """
    r = rng or random
    metrics = []

    is_drop = phase == "drop_live"
    is_pre = phase == "pre_drop"

    # Base request rates vary by phase
    base_rate = 5 if is_pre else (200 if is_drop else 15)

    # ── snkrs.requests — per endpoint/status/region ──────────
    for endpoint in ENDPOINTS:
        for region in REGIONS:
            region_weight = {"us-east": 0.40, "us-west": 0.25, "eu-west": 0.20,
                             "ap-southeast": 0.10, "ap-northeast": 0.05}[region]

            # Most requests are 200/201
            success_count = max(0, int(base_rate * region_weight * r.uniform(0.8, 1.2)))

            if success_count > 0:
                status = "201" if endpoint.startswith("POST") else "200"
                metrics.append(_make_metric(
                    ts, "snkrs.requests", "Total API requests", "1", "Sum",
                    float(success_count),
                    {"endpoint": endpoint, "status": status, "region": region}))

            # Error counts (higher during incidents)
            err_rate = 0.02 if not is_drop else 0.08
            if incident:
                if endpoint in incident.get("affected_endpoints", []):
                    err_rate = incident["error_rate_override"]
            err_count = max(0, int(success_count * err_rate * r.uniform(0.5, 2.0)))
            if err_count > 0:
                err_status = r.choice(["500", "503", "429", "402"])
                metrics.append(_make_metric(
                    ts, "snkrs.requests", "Total API requests", "1", "Sum",
                    float(err_count),
                    {"endpoint": endpoint, "status": err_status, "region": region}))

    # ── snkrs.inventory — per product/size ──────────
    if drop_product:
        from inventory import SIZES, SIZE_WEIGHTS
        for size in SIZES:
            weight = SIZE_WEIGHTS[size]
            size_stock = max(0, int(stock_remaining * weight * r.uniform(0.8, 1.2)))
            metrics.append(_make_metric(
                ts, "snkrs.inventory", "Remaining inventory", "1", "Gauge",
                float(size_stock),
                {"product.id": drop_product["id"], "product.sku": drop_product["sku"], "shoe.size": size}))

    # ── snkrs.queue_depth ──────────
    metrics.append(_make_metric(
        ts, "snkrs.queue_depth", "Draw queue depth", "1", "Gauge",
        float(queue_depth),
        {"product.id": drop_product["id"] if drop_product else "none"}))

    # ── snkrs.errors — per endpoint/error_type ──────────
    for endpoint in ENDPOINTS:
        for err_type in ERROR_TYPES:
            # Only some error types apply to some endpoints
            if err_type == "payment" and "checkout" not in endpoint:
                continue
            if err_type == "inventory" and "checkout" not in endpoint:
                continue
            if err_type == "fraud" and "checkout" not in endpoint:
                continue

            err_base = 0 if is_pre else (r.randint(0, 5) if is_drop else r.randint(0, 1))
            if incident and endpoint in incident.get("affected_endpoints", []):
                err_base = int(err_base * 3 * r.uniform(1, 2))

            if err_base > 0:
                metrics.append(_make_metric(
                    ts, "snkrs.errors", "Total errors", "1", "Sum",
                    float(err_base),
                    {"endpoint": endpoint, "error_type": err_type}))

    # ── snkrs.payments — per status ──────────
    if is_drop or (not is_pre and r.random() < 0.3):
        for pay_status in PAYMENT_STATUSES:
            if pay_status == "success":
                count = int(base_rate * 0.15 * r.uniform(0.8, 1.2))
            elif pay_status == "failed":
                count = int(base_rate * 0.03 * r.uniform(0.5, 2.0))
            else:
                count = int(base_rate * 0.01 * r.uniform(0.5, 2.0))

            if incident and incident.get("affected_service") == "payment-service":
                if pay_status == "failed":
                    count = int(count * 5)

            if count > 0:
                metrics.append(_make_metric(
                    ts, "snkrs.payments", "Payment attempts", "1", "Sum",
                    float(count),
                    {"status": pay_status, "product.id": drop_product["id"] if drop_product else "none"}))

    # ── snkrs.latency_p99 — per service ──────────
    for service in SERVICES:
        base_latency = {
            "api-gateway": 50, "auth-service": 30, "product-service": 40,
            "draw-service": 60, "inventory-service": 35, "payment-service": 250,
            "order-service": 80, "notification-service": 120,
        }[service]

        mult = 1.0
        if is_drop:
            mult = r.uniform(1.5, 3.0)
        if incident:
            if incident.get("affected_service") in ("all", service):
                mult *= incident["latency_multiplier"]

        latency = base_latency * mult * r.uniform(0.8, 1.2)
        metrics.append(_make_metric(
            ts, "snkrs.latency_p99", "P99 latency per service", "ms", "Gauge",
            round(latency, 1),
            {"service.name": service}))

    # ── snkrs.active_users — per region/device ──────────
    for region in REGIONS:
        for device in DEVICES:
            region_weight = {"us-east": 0.40, "us-west": 0.25, "eu-west": 0.20,
                             "ap-southeast": 0.10, "ap-northeast": 0.05}[region]
            device_weight = {"ios": 0.55, "android": 0.30, "web": 0.15}[device]
            users = max(0, int(active_users * region_weight * device_weight * r.uniform(0.7, 1.3)))

            if incident and incident.get("affected_region") == region:
                users = 0  # regional outage

            if users > 0:
                metrics.append(_make_metric(
                    ts, "snkrs.active_users", "Active users", "1", "Gauge",
                    float(users),
                    {"region": region, "device": device}))

    return metrics


def _make_metric(ts: datetime, name: str, description: str, unit: str,
                 metric_type: str, value: float, attrs: dict) -> dict:
    """Create a single metric row matching the otel_metrics schema."""
    return {
        "Timestamp": ts,
        "MetricName": name,
        "MetricDescription": description,
        "MetricUnit": unit,
        "MetricType": metric_type,
        "Value": value,
        "ServiceName": "snkrs-platform",
        "ResourceAttributes": RESOURCE_ATTRS.copy(),
        "MetricAttributes": {k: str(v) for k, v in attrs.items()},
    }
