-- ============================================================
--  Nike SNKRS Drop — ClickHouse Query Playbook
--  Paste these in Tabix: http://localhost:8080
-- ============================================================


-- ════════════════════════════════════════════════════════════
--  1. LIVE DROP DASHBOARD
--     Run during the DROP_LIVE phase to watch chaos unfold
-- ════════════════════════════════════════════════════════════

-- Requests per second by endpoint (last 2 minutes)
SELECT
    toStartOfInterval(Timestamp, INTERVAL 10 SECOND) AS bucket,
    SpanName                                          AS endpoint,
    count()                                           AS rps,
    countIf(StatusCode = 'STATUS_CODE_ERROR')         AS errors
FROM otel.otel_traces
WHERE ParentSpanId = ''   -- root spans only (one per HTTP request)
  AND Timestamp >= now() - INTERVAL 2 MINUTE
GROUP BY bucket, endpoint
ORDER BY bucket DESC, rps DESC;

-- Error rate right now by endpoint
SELECT
    SpanName                                           AS endpoint,
    count()                                            AS total,
    countIf(StatusCode = 'STATUS_CODE_ERROR')          AS errors,
    round(errors / total * 100, 1)                     AS error_pct
FROM otel.otel_traces
WHERE ParentSpanId = ''
  AND Timestamp >= now() - INTERVAL 5 MINUTE
GROUP BY endpoint
ORDER BY error_pct DESC;

-- P99 latency by endpoint (last 5 min)
SELECT
    SpanName                                           AS endpoint,
    count()                                            AS requests,
    round(quantile(0.50)(Duration) / 1e6, 1)          AS p50_ms,
    round(quantile(0.95)(Duration) / 1e6, 1)          AS p95_ms,
    round(quantile(0.99)(Duration) / 1e6, 1)          AS p99_ms,
    round(max(Duration) / 1e6, 1)                     AS max_ms
FROM otel.otel_traces
WHERE ParentSpanId = ''
  AND Timestamp >= now() - INTERVAL 5 MINUTE
GROUP BY endpoint
ORDER BY p99_ms DESC;


-- ════════════════════════════════════════════════════════════
--  2. MICROSERVICE INTERNALS
--     Which internal service is causing slowdowns?
-- ════════════════════════════════════════════════════════════

-- All internal spans ranked by avg latency
SELECT
    SpanName                                           AS span,
    count()                                            AS calls,
    round(avg(Duration) / 1e6, 1)                     AS avg_ms,
    round(quantile(0.95)(Duration) / 1e6, 1)          AS p95_ms,
    countIf(StatusCode = 'STATUS_CODE_ERROR')          AS errors
FROM otel.otel_traces
WHERE ParentSpanId != ''   -- child spans only (internal calls)
  AND Timestamp >= now() - INTERVAL 10 MINUTE
GROUP BY span
ORDER BY p95_ms DESC
LIMIT 30;

-- DB queries specifically — where is DB time going?
SELECT
    SpanName                                           AS query,
    count()                                            AS calls,
    round(avg(Duration) / 1e6, 1)                     AS avg_ms,
    round(quantile(0.99)(Duration) / 1e6, 1)          AS p99_ms
FROM otel.otel_traces
WHERE (SpanName LIKE 'db.%' OR SpanName LIKE '%.postgres%' OR SpanName LIKE '%.dynamo%')
  AND Timestamp >= now() - INTERVAL 10 MINUTE
GROUP BY query
ORDER BY p99_ms DESC;

-- Cache hit rate (redis calls)
SELECT
    SpanName,
    count()                                            AS total_calls,
    round(avg(Duration) / 1e6, 1)                     AS avg_ms
FROM otel.otel_traces
WHERE SpanName LIKE '%redis%' OR SpanName LIKE '%cache%'
  AND Timestamp >= now() - INTERVAL 10 MINUTE
GROUP BY SpanName
ORDER BY total_calls DESC;


-- ════════════════════════════════════════════════════════════
--  3. DROP PHASE COMPARISON
--     PRE_DROP vs DROP_LIVE vs POST_DROP side by side
-- ════════════════════════════════════════════════════════════

SELECT
    SpanAttributes['drop.phase']                       AS phase,
    count()                                            AS requests,
    round(avg(Duration) / 1e6, 1)                     AS avg_ms,
    round(quantile(0.99)(Duration) / 1e6, 1)          AS p99_ms,
    countIf(StatusCode = 'STATUS_CODE_ERROR')          AS errors,
    round(errors / requests * 100, 1)                  AS error_pct
FROM otel.otel_traces
WHERE ParentSpanId = ''
  AND SpanAttributes['drop.phase'] != ''
GROUP BY phase
ORDER BY phase;

-- Traffic spike visualised — requests per 10 seconds coloured by phase
SELECT
    toStartOfInterval(Timestamp, INTERVAL 10 SECOND) AS bucket,
    SpanAttributes['drop.phase']                      AS phase,
    count()                                           AS requests
FROM otel.otel_traces
WHERE ParentSpanId = ''
GROUP BY bucket, phase
ORDER BY bucket;


-- ════════════════════════════════════════════════════════════
--  4. FAILURE ANALYSIS
-- ════════════════════════════════════════════════════════════

-- What errors are happening and where?
SELECT
    SpanName                                           AS where_it_failed,
    SpanAttributes['drop.phase']                       AS phase,
    count()                                            AS occurrences
FROM otel.otel_traces
WHERE StatusCode = 'STATUS_CODE_ERROR'
GROUP BY where_it_failed, phase
ORDER BY occurrences DESC
LIMIT 20;

-- Bot detection events
SELECT
    Timestamp,
    Body                                               AS log_message,
    TraceId
FROM otel.otel_logs
WHERE Body LIKE '%bot%' OR Body LIKE '%flagged%'
ORDER BY Timestamp DESC
LIMIT 20;

-- Sold-out errors over time
SELECT
    toStartOfInterval(Timestamp, INTERVAL 10 SECOND) AS bucket,
    count()                                           AS sold_out_hits
FROM otel.otel_logs
WHERE Body LIKE '%SOLD OUT%'
GROUP BY bucket
ORDER BY bucket;

-- Payment failures breakdown
SELECT
    Body                                               AS error,
    count()                                            AS occurrences
FROM otel.otel_logs
WHERE Body LIKE '%Payment%' OR Body LIKE '%Stripe%' OR Body LIKE '%fraud%'
GROUP BY error
ORDER BY occurrences DESC;


-- ════════════════════════════════════════════════════════════
--  5. DRILL INTO A SINGLE CHECKOUT TRACE
--     Pick a failed checkout and see exactly where it broke
-- ════════════════════════════════════════════════════════════

-- Step 1: Find a failed checkout TraceId
SELECT
    TraceId,
    round(Duration / 1e6, 1) AS total_ms,
    SpanAttributes['http.status_code'] AS status
FROM otel.otel_traces
WHERE SpanName = 'POST /v1/checkout'
  AND StatusCode = 'STATUS_CODE_ERROR'
ORDER BY Timestamp DESC
LIMIT 10;

-- Step 2: Paste a TraceId below to see the full span tree
SELECT
    SpanName,
    SpanId,
    ParentSpanId,
    if(ParentSpanId = '', '► ROOT', '  └─ ') AS level,
    round(Duration / 1e6, 1)                 AS duration_ms,
    StatusCode,
    Timestamp
FROM otel.otel_traces
WHERE TraceId = 'paste-trace-id-here'
ORDER BY Timestamp ASC;


-- ════════════════════════════════════════════════════════════
--  6. REGIONAL BREAKDOWN
-- ════════════════════════════════════════════════════════════

SELECT
    SpanAttributes['cloud.region']                     AS region,
    count()                                            AS requests,
    round(avg(Duration) / 1e6, 1)                     AS avg_ms,
    countIf(StatusCode = 'STATUS_CODE_ERROR')          AS errors,
    round(errors / requests * 100, 1)                  AS error_pct
FROM otel.otel_traces
WHERE ParentSpanId = ''
GROUP BY region
ORDER BY requests DESC;

-- Device split
SELECT
    SpanAttributes['device.type']                      AS device,
    count()                                            AS requests,
    countIf(StatusCode = 'STATUS_CODE_ERROR')          AS errors
FROM otel.otel_traces
WHERE ParentSpanId = ''
GROUP BY device
ORDER BY requests DESC;


-- ════════════════════════════════════════════════════════════
--  7. STORAGE STATS
-- ════════════════════════════════════════════════════════════

SELECT
    table,
    formatReadableQuantity(sum(rows))                  AS rows,
    formatReadableSize(sum(data_compressed_bytes))     AS on_disk,
    formatReadableSize(sum(data_uncompressed_bytes))   AS uncompressed,
    round(sum(data_compressed_bytes) /
          sum(data_uncompressed_bytes) * 100, 1)       AS compression_pct
FROM system.parts
WHERE database = 'otel' AND active
GROUP BY table
ORDER BY sum(rows) DESC;