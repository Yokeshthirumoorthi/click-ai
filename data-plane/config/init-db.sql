-- ============================================================
--  Custom ClickHouse tables for the S3-first pipeline
--  Run once on startup (mounted as init script)
-- ============================================================

CREATE DATABASE IF NOT EXISTS otel;

-- ── otel_traces ───────────────────────────────────────────────
-- Schema for the S3-first pipeline. The s3-loader writes to this table.
CREATE TABLE IF NOT EXISTS otel.otel_traces (
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
    )                                                          CODEC(ZSTD(1)),

    INDEX idx_trace_id        TraceId                     TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key    mapKeys(ResourceAttributes) TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_res_attr_value  mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_key   mapKeys(SpanAttributes)     TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes)   TYPE bloom_filter(0.01)  GRANULARITY 1,
    INDEX idx_duration        Duration                    TYPE minmax              GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDateTime(Timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- ── Trace ID → Timestamp lookup ─────────────────────────────
CREATE TABLE IF NOT EXISTS otel.otel_traces_trace_id_ts (
    TraceId  String   CODEC(ZSTD(1)),
    Start    DateTime CODEC(Delta, ZSTD(1)),
    End      DateTime CODEC(Delta, ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toDate(Start)
ORDER BY (TraceId, Start)
TTL Start + INTERVAL 30 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS otel.otel_traces_trace_id_ts_mv
TO otel.otel_traces_trace_id_ts
AS SELECT
    TraceId,
    min(Timestamp) AS Start,
    max(Timestamp) AS End
FROM otel.otel_traces
WHERE TraceId != ''
GROUP BY TraceId;

-- ── otel_logs ────────────────────────────────────────────────
-- Log records from the S3-first pipeline. TraceId/SpanId enable
-- log↔trace correlation when logs are emitted inside active spans.
CREATE TABLE IF NOT EXISTS otel.otel_logs (
    Timestamp          DateTime64(9)                           CODEC(Delta, ZSTD(1)),
    TraceId            String                                  CODEC(ZSTD(1)),
    SpanId             String                                  CODEC(ZSTD(1)),
    SeverityNumber     UInt8                                   CODEC(ZSTD(1)),
    SeverityText       LowCardinality(String)                  CODEC(ZSTD(1)),
    Body               String                                  CODEC(ZSTD(1)),
    ServiceName        LowCardinality(String)                  CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
    LogAttributes      Map(LowCardinality(String), String)     CODEC(ZSTD(1)),

    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_body     Body    TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, toDateTime(Timestamp))
TTL toDateTime(Timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- ── otel_metrics ─────────────────────────────────────────────
-- Metric data points. No TraceId — metrics are aggregated.
-- Correlate with traces via ServiceName + time window.
CREATE TABLE IF NOT EXISTS otel.otel_metrics (
    Timestamp          DateTime64(9)                           CODEC(Delta, ZSTD(1)),
    MetricName         LowCardinality(String)                  CODEC(ZSTD(1)),
    MetricDescription  String                                  CODEC(ZSTD(1)),
    MetricUnit         String                                  CODEC(ZSTD(1)),
    MetricType         LowCardinality(String)                  CODEC(ZSTD(1)),
    Value              Float64                                 CODEC(ZSTD(1)),
    ServiceName        LowCardinality(String)                  CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String)     CODEC(ZSTD(1)),
    MetricAttributes   Map(LowCardinality(String), String)     CODEC(ZSTD(1)),

    INDEX idx_metric_name MetricName TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, MetricName, toDateTime(Timestamp))
TTL toDateTime(Timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- ── Enriched traces (with embeddings) ─────────────────────────
CREATE TABLE IF NOT EXISTS otel.otel_traces_enriched (
    Timestamp          DateTime64(9)              CODEC(Delta, ZSTD(1)),
    TraceId            String                     CODEC(ZSTD(1)),
    SpanId             String                     CODEC(ZSTD(1)),
    ParentSpanId       String                     CODEC(ZSTD(1)),
    SpanName           LowCardinality(String)     CODEC(ZSTD(1)),
    SpanKind           LowCardinality(String)     CODEC(ZSTD(1)),
    ServiceName        LowCardinality(String)     CODEC(ZSTD(1)),
    Duration           UInt64                     CODEC(ZSTD(1)),
    StatusCode         LowCardinality(String)     CODEC(ZSTD(1)),
    StatusMessage      String                     CODEC(ZSTD(1)),
    -- Flattened attributes (top-level columns for fast filtering)
    ResourceAttributesFlat Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    SpanAttributesFlat     Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    -- Embedding columns
    EmbeddingText      String                     CODEC(ZSTD(1)),
    Embedding          Array(Float32)             CODEC(ZSTD(1)),

    INDEX idx_trace_id  TraceId  TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_duration  Duration TYPE minmax              GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDateTime(Timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- ── Loader file watermark ─────────────────────────────────────
-- Tracks which S3 files have been processed by the loader.
-- ReplacingMergeTree deduplicates by filename on merge.
CREATE TABLE IF NOT EXISTS otel.loader_file_watermark (
    Filename      String,
    Status        LowCardinality(String),  -- 'done' or 'failed'
    ProcessedAt   DateTime DEFAULT now(),
    RowCount      UInt64 DEFAULT 0,
    ErrorMessage  String DEFAULT ''
) ENGINE = ReplacingMergeTree(ProcessedAt)
ORDER BY Filename;

-- ── Log loader file watermark ────────────────────────────────
CREATE TABLE IF NOT EXISTS otel.log_loader_file_watermark (
    Filename      String,
    Status        LowCardinality(String),
    ProcessedAt   DateTime DEFAULT now(),
    RowCount      UInt64 DEFAULT 0,
    ErrorMessage  String DEFAULT ''
) ENGINE = ReplacingMergeTree(ProcessedAt)
ORDER BY Filename;

-- ── Metric loader file watermark ────────────────────────────
CREATE TABLE IF NOT EXISTS otel.metric_loader_file_watermark (
    Filename      String,
    Status        LowCardinality(String),
    ProcessedAt   DateTime DEFAULT now(),
    RowCount      UInt64 DEFAULT 0,
    ErrorMessage  String DEFAULT ''
) ENGINE = ReplacingMergeTree(ProcessedAt)
ORDER BY Filename;

-- ── Enricher watermark ────────────────────────────────────────
-- Tracks row-level progress of the embedding enricher.
CREATE TABLE IF NOT EXISTS otel.enricher_watermark (
    WatermarkKey  String DEFAULT 'global',
    LastTimestamp DateTime64(9),
    LastSpanId    String,
    UpdatedAt     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(UpdatedAt)
ORDER BY WatermarkKey;
