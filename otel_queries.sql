-- ============================================================
--  Query your OTEL data in ClickHouse
--  Run these in Tabix (http://localhost:8080) after starting
--  send_test_events.py for a minute or two
-- ============================================================

-- ── First: see what tables were auto-created ────────────────
SHOW DATABASES;
SHOW TABLES IN otel;

-- ── See the auto-created schema ─────────────────────────────
DESCRIBE TABLE otel.otel_logs;
DESCRIBE TABLE otel.otel_traces;
DESCRIBE TABLE otel.otel_metrics;


-- ════════════════════════════════════════════════════════════
--  LOGS queries
-- ════════════════════════════════════════════════════════════

-- Recent logs
SELECT
    Timestamp,
    SeverityText,              -- INFO / WARN / ERROR
    Body,                      -- the actual log message
    ServiceName
FROM otel.otel_logs
ORDER BY Timestamp DESC
LIMIT 50;

-- Error rate by severity over time
SELECT
    toStartOfMinute(Timestamp) AS minute,
    SeverityText,
    count()                    AS count
FROM otel.otel_logs
GROUP BY minute, SeverityText
ORDER BY minute DESC
LIMIT 100;

-- Only errors
SELECT Timestamp, Body, ServiceName
FROM otel.otel_logs
WHERE SeverityText = 'ERROR'
ORDER BY Timestamp DESC
LIMIT 20;


-- ════════════════════════════════════════════════════════════
--  TRACES queries
-- ════════════════════════════════════════════════════════════

-- Recent traces
SELECT
    Timestamp,
    TraceId,
    SpanId,
    SpanName,                  -- e.g. "HTTP GET /api/users"
    Duration,                  -- in nanoseconds!
    round(Duration / 1e6, 2)   AS duration_ms,   -- convert to ms
    ServiceName,
    StatusCode
FROM otel.otel_traces
ORDER BY Timestamp DESC
LIMIT 50;

-- Slowest endpoints (p95 latency)
SELECT
    SpanName                           AS endpoint,
    count()                            AS requests,
    round(avg(Duration) / 1e6, 2)     AS avg_ms,
    round(quantile(0.95)(Duration) / 1e6, 2) AS p95_ms,
    round(quantile(0.99)(Duration) / 1e6, 2) AS p99_ms
FROM otel.otel_traces
GROUP BY endpoint
ORDER BY p95_ms DESC;

-- Error rate by endpoint
SELECT
    SpanName                                       AS endpoint,
    count()                                        AS total,
    countIf(StatusCode = 'STATUS_CODE_ERROR')      AS errors,
    round(errors / total * 100, 1)                 AS error_pct
FROM otel.otel_traces
GROUP BY endpoint
ORDER BY error_pct DESC;

-- Throughput per minute
SELECT
    toStartOfMinute(Timestamp) AS minute,
    count()                    AS requests_per_minute
FROM otel.otel_traces
GROUP BY minute
ORDER BY minute DESC
LIMIT 60;


-- ════════════════════════════════════════════════════════════
--  METRICS queries
-- ════════════════════════════════════════════════════════════

-- What metrics are being collected?
SELECT DISTINCT MetricName
FROM otel.otel_metrics
ORDER BY MetricName;

-- Request count over time
SELECT
    toStartOfMinute(TimeUnix) AS minute,
    sum(Value)                AS total_requests
FROM otel.otel_metrics
WHERE MetricName = 'http.requests'
GROUP BY minute
ORDER BY minute DESC
LIMIT 60;

-- Average response latency over time
SELECT
    toStartOfMinute(TimeUnix)  AS minute,
    round(avg(Value), 2)       AS avg_latency_ms
FROM otel.otel_metrics
WHERE MetricName = 'http.response_time_ms'
GROUP BY minute
ORDER BY minute DESC
LIMIT 60;


-- ════════════════════════════════════════════════════════════
--  Disk usage — how much space is your OTEL data using?
-- ════════════════════════════════════════════════════════════
SELECT
    table,
    formatReadableQuantity(sum(rows))                AS rows,
    formatReadableSize(sum(data_compressed_bytes))   AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_compressed_bytes) / sum(data_uncompressed_bytes) * 100, 1) AS compression_pct
FROM system.parts
WHERE database = 'otel' AND active
GROUP BY table
ORDER BY sum(rows) DESC;