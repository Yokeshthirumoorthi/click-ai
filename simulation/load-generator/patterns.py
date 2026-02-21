"""
Behavioral patterns per user type — session templates with timing, error rates, branching.
"""

PATTERNS = {
    "casual_browser": {
        "pre_drop": [
            ("GET /v1/feed",             {"latency_ms": 40,  "error_rate": 0.005}),
            ("GET /v1/products/{id}",    {"latency_ms": 100, "error_rate": 0.01, "repeat": (1, 2)}),
            ("GET /v1/account/profile",  {"latency_ms": 60,  "error_rate": 0.01}),
        ],
        "drop_live": [
            ("GET /v1/products/{id}",    {"latency_ms": 150, "error_rate": 0.03}),
            ("POST /v1/draw/enter",      {"latency_ms": 250, "error_rate": 0.10, "probability": 0.30}),
        ],
        "post_drop": [
            ("GET /v1/feed",             {"latency_ms": 35,  "error_rate": 0.005}),
            ("GET /v1/account/profile",  {"latency_ms": 50,  "error_rate": 0.01, "probability": 0.40}),
        ],
        "session_gap_ms": (2000, 8000),
        "sessions_per_drop": (1, 2),
        "checkout_probability": 0.05,
    },

    "hype_buyer": {
        "pre_drop": [
            ("GET /v1/account/profile",  {"latency_ms": 80,  "error_rate": 0.01}),
            ("GET /v1/products/{id}",    {"latency_ms": 120, "error_rate": 0.02, "repeat": (2, 4)}),
        ],
        "drop_live": [
            ("POST /v1/draw/enter",      {"latency_ms": 200, "error_rate": 0.05, "retry": 2}),
            ("POST /v1/checkout",        {"latency_ms": 800, "error_rate": 0.08}),
        ],
        "post_drop": [
            ("GET /v1/account/profile",  {"latency_ms": 60,  "error_rate": 0.01}),
        ],
        "session_gap_ms": (500, 2000),
        "sessions_per_drop": (1, 3),
        "checkout_probability": 0.70,
    },

    "reseller": {
        "pre_drop": [
            ("GET /v1/products/{id}",    {"latency_ms": 60,  "error_rate": 0.02, "repeat": (3, 6)}),
        ],
        "drop_live": [
            ("POST /v1/draw/enter",      {"latency_ms": 50,  "error_rate": 0.15, "retry": 5}),
            ("POST /v1/checkout",        {"latency_ms": 300, "error_rate": 0.20}),
        ],
        "post_drop": [
            ("GET /v1/account/profile",  {"latency_ms": 40,  "error_rate": 0.02}),
        ],
        "session_gap_ms": (100, 500),
        "sessions_per_drop": (3, 8),
        "checkout_probability": 0.85,
        "bot_flag_rate": 0.30,
    },

    "window_shopper": {
        "pre_drop": [
            ("GET /v1/feed",             {"latency_ms": 35,  "error_rate": 0.003}),
            ("GET /v1/products/{id}",    {"latency_ms": 90,  "error_rate": 0.01, "repeat": (2, 5)}),
        ],
        "drop_live": [
            ("GET /v1/products/{id}",    {"latency_ms": 130, "error_rate": 0.03, "repeat": (1, 3)}),
            ("GET /v1/feed",             {"latency_ms": 50,  "error_rate": 0.01}),
        ],
        "post_drop": [
            ("GET /v1/feed",             {"latency_ms": 30,  "error_rate": 0.003}),
        ],
        "session_gap_ms": (3000, 10000),
        "sessions_per_drop": (1, 2),
        "checkout_probability": 0.0,
    },

    "new_user": {
        "pre_drop": [
            ("GET /v1/account/profile",  {"latency_ms": 120, "error_rate": 0.05}),
            ("GET /v1/feed",             {"latency_ms": 80,  "error_rate": 0.02}),
            ("GET /v1/products/{id}",    {"latency_ms": 150, "error_rate": 0.03, "repeat": (1, 3)}),
        ],
        "drop_live": [
            ("POST /v1/draw/enter",      {"latency_ms": 400, "error_rate": 0.15}),
            ("POST /v1/checkout",        {"latency_ms": 1200, "error_rate": 0.25}),
        ],
        "post_drop": [
            ("GET /v1/account/profile",  {"latency_ms": 100, "error_rate": 0.03}),
        ],
        "session_gap_ms": (3000, 15000),
        "sessions_per_drop": (1, 2),
        "checkout_probability": 0.30,
    },
}


# ── Incident types injected every Nth drop ──────────────────────────────

INCIDENTS = [
    {
        "name": "connection_pool_exhaustion",
        "description": "Payment service connection pool exhausted",
        "affected_service": "payment-service",
        "affected_endpoints": ["POST /v1/checkout"],
        "error_rate_override": 0.40,
        "latency_multiplier": 3.0,
        "duration_fraction": 0.25,  # affects 25% of the drop window
        "error_message": "Connection pool exhausted: max connections reached (50/50)",
    },
    {
        "name": "inventory_race_condition",
        "description": "Overselling due to race condition — stock goes negative",
        "affected_service": "inventory-service",
        "affected_endpoints": ["POST /v1/checkout"],
        "error_rate_override": 0.15,
        "latency_multiplier": 2.0,
        "duration_fraction": 0.40,
        "error_message": "Inventory consistency error: stock negative after concurrent decrement",
    },
    {
        "name": "bot_storm",
        "description": "Bot storm triggers auth-service rate limiting",
        "affected_service": "auth-service",
        "affected_endpoints": ["POST /v1/draw/enter", "POST /v1/checkout"],
        "error_rate_override": 0.30,
        "latency_multiplier": 1.5,
        "duration_fraction": 0.50,
        "error_message": "Request blocked: bot detection triggered (fingerprint mismatch)",
    },
    {
        "name": "cdn_outage",
        "description": "CDN provider experiencing regional outage",
        "affected_service": "product-service",
        "affected_endpoints": ["GET /v1/products/{id}", "GET /v1/feed"],
        "error_rate_override": 0.20,
        "latency_multiplier": 10.0,
        "duration_fraction": 0.35,
        "error_message": "CDN timeout: upstream provider returned 504 after 30s",
    },
    {
        "name": "database_failover",
        "description": "Primary DB failover to replica — 3s latency spike",
        "affected_service": "all",
        "affected_endpoints": ["POST /v1/checkout", "POST /v1/draw/enter", "GET /v1/account/profile"],
        "error_rate_override": 0.10,
        "latency_multiplier": 15.0,
        "duration_fraction": 0.10,
        "error_message": "Database failover in progress: read-only replica promoted, connections draining",
    },
    {
        "name": "regional_outage",
        "description": "Complete regional outage in eu-west",
        "affected_service": "all",
        "affected_region": "eu-west",
        "affected_endpoints": ["POST /v1/checkout", "POST /v1/draw/enter", "GET /v1/products/{id}", "GET /v1/feed", "GET /v1/account/profile"],
        "error_rate_override": 1.0,
        "latency_multiplier": 1.0,
        "duration_fraction": 0.45,
        "error_message": "Regional infrastructure failure: eu-west-1 availability zone unreachable",
    },
]


def get_incident_for_drop(drop_index: int) -> dict | None:
    """Return an incident for this drop, or None. Every 5th drop gets one."""
    if drop_index % 5 != 3:  # drops 3, 8, 13, 18, ...
        return None
    incident_idx = (drop_index // 5) % len(INCIDENTS)
    return INCIDENTS[incident_idx]


def should_apply_incident(incident: dict, service: str, endpoint: str,
                          region: str, progress: float) -> bool:
    """Check if this incident should affect the current span."""
    if incident is None:
        return False

    # Check time window
    start = 0.5 - incident["duration_fraction"] / 2
    end = 0.5 + incident["duration_fraction"] / 2
    if progress < start or progress > end:
        return False

    # Check region for regional outages
    if "affected_region" in incident and region != incident["affected_region"]:
        return False

    # Check service
    if incident["affected_service"] != "all" and service != incident["affected_service"]:
        return False

    # Check endpoint
    if endpoint not in incident["affected_endpoints"]:
        return False

    return True
