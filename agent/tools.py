"""
Tool definitions and implementations for the LLM agent.
Each tool queries ClickHouse and returns structured results.
"""

import json
from datetime import datetime, timedelta

import clickhouse_connect
from sentence_transformers import SentenceTransformer

import config

# Lazy-init globals
_ch_client = None
_model = None


def get_ch():
    global _ch_client
    if _ch_client is None:
        _ch_client = clickhouse_connect.get_client(
            host=config.CH_HOST,
            port=8123,
            username=config.CH_USER,
            password=config.CH_PASSWORD,
            database=config.CH_DATABASE,
        )
    return _ch_client


def get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(config.EMBEDDING_MODEL)
    return _model


# ── Tool schemas (for Claude tool_use) ─────────────────────────

TOOL_DEFINITIONS = [
    {
        "name": "query_spans_by_time",
        "description": (
            "Query raw trace spans from ClickHouse with time range and optional filters. "
            "Returns matching spans with key fields. Use for time-based or attribute-filtered queries."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "minutes_ago": {
                    "type": "integer",
                    "description": "Look back this many minutes from now. Default 60.",
                    "default": 60,
                },
                "service_name": {
                    "type": "string",
                    "description": "Filter by service name (exact match). Optional.",
                },
                "span_name": {
                    "type": "string",
                    "description": "Filter by span name (LIKE pattern, use % for wildcards). Optional.",
                },
                "min_duration_ms": {
                    "type": "number",
                    "description": "Minimum span duration in milliseconds. Optional.",
                },
                "status_code": {
                    "type": "string",
                    "description": "Filter by status code: STATUS_CODE_OK, STATUS_CODE_ERROR, STATUS_CODE_UNSET. Optional.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows to return. Default 50.",
                    "default": 50,
                },
            },
            "required": [],
        },
    },
    {
        "name": "find_similar_spans",
        "description": (
            "Semantic search: find spans similar to a natural language description. "
            "Uses vector embeddings and cosineDistance for similarity matching. "
            "Best for conceptual queries like 'slow payment processing' or 'failed auth attempts'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language description of the spans to find.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return. Default 20.",
                    "default": 20,
                },
                "min_score": {
                    "type": "number",
                    "description": "Minimum similarity score (0-1, higher = more similar). Default 0.3.",
                    "default": 0.3,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_span_details",
        "description": (
            "Get all spans for a specific trace ID, showing the full trace tree. "
            "Use to drill into a specific trace after finding it via other tools."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "trace_id": {
                    "type": "string",
                    "description": "The trace ID to look up.",
                },
            },
            "required": ["trace_id"],
        },
    },
    {
        "name": "embed_text",
        "description": (
            "Get the raw embedding vector for a text string. "
            "Useful for debugging or custom similarity computations."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "The text to embed.",
                },
            },
            "required": ["text"],
        },
    },
]


# ── Tool implementations ───────────────────────────────────────

def query_spans_by_time(
    minutes_ago: int = 60,
    service_name: str | None = None,
    span_name: str | None = None,
    min_duration_ms: float | None = None,
    status_code: str | None = None,
    limit: int = 50,
) -> str:
    ch = get_ch()

    conditions = ["Timestamp >= {ts:DateTime64(9)}"]
    params: dict = {
        "ts": datetime.utcnow() - timedelta(minutes=minutes_ago),
        "limit": min(limit, 200),
    }

    if service_name:
        conditions.append("ServiceName = {svc:String}")
        params["svc"] = service_name
    if span_name:
        conditions.append("SpanName LIKE {span:String}")
        params["span"] = span_name
    if min_duration_ms is not None:
        conditions.append("Duration >= {min_dur:UInt64}")
        params["min_dur"] = int(min_duration_ms * 1_000_000)
    if status_code:
        conditions.append("StatusCode = {sc:String}")
        params["sc"] = status_code

    where = " AND ".join(conditions)

    query = f"""
    SELECT
        Timestamp,
        TraceId,
        SpanId,
        ParentSpanId,
        ServiceName,
        SpanName,
        SpanKind,
        Duration / 1000000 AS DurationMs,
        StatusCode,
        StatusMessage,
        SpanAttributes
    FROM otel_traces
    WHERE {where}
    ORDER BY Timestamp DESC
    LIMIT {{limit:UInt32}}
    """

    result = ch.query(query, parameters=params)
    rows = []
    for row in result.result_rows:
        rows.append(dict(zip(result.column_names, [
            str(v) if isinstance(v, datetime) else v
            for v in row
        ])))

    return json.dumps({"count": len(rows), "spans": rows}, default=str)


def find_similar_spans(
    query: str,
    limit: int = 20,
    min_score: float = 0.3,
) -> str:
    ch = get_ch()
    model = get_model()

    # Encode the query
    embedding = model.encode(query).tolist()

    sql = """
    SELECT
        Timestamp,
        TraceId,
        SpanId,
        ServiceName,
        SpanName,
        SpanKind,
        Duration / 1000000 AS DurationMs,
        StatusCode,
        StatusMessage,
        EmbeddingText,
        1 - cosineDistance(Embedding, {emb:Array(Float32)}) AS similarity
    FROM otel_traces_enriched
    WHERE similarity >= {min_score:Float32}
    ORDER BY similarity DESC
    LIMIT {limit:UInt32}
    """

    result = ch.query(sql, parameters={
        "emb": embedding,
        "min_score": min_score,
        "limit": min(limit, 100),
    })

    rows = []
    for row in result.result_rows:
        rows.append(dict(zip(result.column_names, [
            str(v) if isinstance(v, datetime) else v
            for v in row
        ])))

    return json.dumps({"query": query, "count": len(rows), "spans": rows}, default=str)


def get_span_details(trace_id: str) -> str:
    ch = get_ch()

    result = ch.query(
        """
        SELECT
            Timestamp,
            TraceId,
            SpanId,
            ParentSpanId,
            ServiceName,
            SpanName,
            SpanKind,
            Duration / 1000000 AS DurationMs,
            StatusCode,
            StatusMessage,
            SpanAttributes,
            ResourceAttributes
        FROM otel_traces
        WHERE TraceId = {tid:String}
        ORDER BY Timestamp ASC
        """,
        parameters={"tid": trace_id},
    )

    rows = []
    for row in result.result_rows:
        rows.append(dict(zip(result.column_names, [
            str(v) if isinstance(v, datetime) else v
            for v in row
        ])))

    return json.dumps({"trace_id": trace_id, "span_count": len(rows), "spans": rows}, default=str)


def embed_text(text: str) -> str:
    model = get_model()
    embedding = model.encode(text).tolist()
    return json.dumps({"text": text, "dimensions": len(embedding), "embedding": embedding[:10] + ["..."]})


# ── Tool dispatch ──────────────────────────────────────────────

TOOL_HANDLERS = {
    "query_spans_by_time": query_spans_by_time,
    "find_similar_spans": find_similar_spans,
    "get_span_details": get_span_details,
    "embed_text": embed_text,
}


def execute_tool(name: str, arguments: dict) -> str:
    handler = TOOL_HANDLERS.get(name)
    if handler is None:
        return json.dumps({"error": f"Unknown tool: {name}"})
    try:
        return handler(**arguments)
    except Exception as e:
        return json.dumps({"error": str(e)})
