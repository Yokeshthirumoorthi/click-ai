-- ============================================================
--  Session ClickHouse — table DDL for per-session databases
--  The session builder creates databases dynamically as:
--    session_<id>.otel_traces / otel_logs / otel_metrics
--
--  Schema must match data-plane/config/init-db.sql exactly
--  (same columns, partition key, order key) so that parts
--  frozen on master can be attached here.
--
--  Differences from master:
--    - Plain MergeTree (not Replicated)
--    - No TTL (session data may be older than 30 days)
--    - No bloom_filter indexes (lightweight)
-- ============================================================

-- otel_traces — identical column layout to master
CREATE TABLE IF NOT EXISTS otel_traces (
    Timestamp          DateTime64(9)                           CODEC(Delta, ZSTD(1)),
    TraceId            String                                  CODEC(ZSTD(1)),
    SpanId             String                                  CODEC(ZSTD(1)),
    ParentSpanId       String                                  CODEC(ZSTD(1)),
    TraceState         String                                  CODEC(ZSTD(1)),
    SpanName           LowCardinality(String)                  CODEC(ZSTD(1)),
    SpanKind           LowCardinality(String)                  CODEC(ZSTD(1)),
    ServiceName        LowCardinality(String)                  CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
    ScopeName          String                                  CODEC(ZSTD(1)),
    ScopeVersion       String                                  CODEC(ZSTD(1)),
    SpanAttributes     Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
    Duration           UInt64                                  CODEC(ZSTD(1)),
    StatusCode         LowCardinality(String)                  CODEC(ZSTD(1)),
    StatusMessage      String                                  CODEC(ZSTD(1)),
    Events Nested (
        Timestamp   DateTime64(9),
        Name        LowCardinality(String),
        Attributes  Map(LowCardinality(String), String)
    )                                                          CODEC(ZSTD(1)),
    Links Nested (
        TraceId     String,
        SpanId      String,
        TraceState  String,
        Attributes  Map(LowCardinality(String), String)
    )                                                          CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS index_granularity = 8192;

-- otel_logs
CREATE TABLE IF NOT EXISTS otel_logs (
    Timestamp          DateTime64(9)                           CODEC(Delta, ZSTD(1)),
    TraceId            String                                  CODEC(ZSTD(1)),
    SpanId             String                                  CODEC(ZSTD(1)),
    SeverityNumber     UInt8                                   CODEC(ZSTD(1)),
    SeverityText       LowCardinality(String)                  CODEC(ZSTD(1)),
    Body               String                                  CODEC(ZSTD(1)),
    ServiceName        LowCardinality(String)                  CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
    LogAttributes      Map(LowCardinality(String), String)     CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, toDateTime(Timestamp))
SETTINGS index_granularity = 8192;

-- otel_metrics
CREATE TABLE IF NOT EXISTS otel_metrics (
    Timestamp          DateTime64(9)                           CODEC(Delta, ZSTD(1)),
    MetricName         LowCardinality(String)                  CODEC(ZSTD(1)),
    MetricDescription  String                                  CODEC(ZSTD(1)),
    MetricUnit         String                                  CODEC(ZSTD(1)),
    MetricType         LowCardinality(String)                  CODEC(ZSTD(1)),
    Value              Float64                                 CODEC(ZSTD(1)),
    ServiceName        LowCardinality(String)                  CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
    MetricAttributes   Map(LowCardinality(String), String)     CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, MetricName, toDateTime(Timestamp))
SETTINGS index_granularity = 8192;
