"""
Embedding Enricher: polls otel_traces for new rows, computes embeddings
with sentence-transformers (all-MiniLM-L6-v2), and writes enriched rows
to otel_traces_enriched.

Architecture:
  Prefetch thread ──queue──▶ Main thread (GPU encode + insert)
    fetch BATCH_SIZE rows      encode batch on GPU
    from ClickHouse            insert enriched rows
    (continuous)               update watermark
"""

import logging
import time
import threading
import queue
from datetime import datetime

import clickhouse_connect
from sentence_transformers import SentenceTransformer

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("enricher")


def get_ch_client():
    return clickhouse_connect.get_client(
        host=config.CH_HOST,
        port=8123,
        username=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DATABASE,
    )


def get_watermark(ch) -> tuple[datetime, str]:
    """Get the current enricher watermark (LastTimestamp, LastSpanId)."""
    result = ch.query(
        "SELECT LastTimestamp, LastSpanId "
        "FROM enricher_watermark FINAL "
        "WHERE WatermarkKey = 'global' "
        "LIMIT 1"
    )
    if result.result_rows:
        return result.result_rows[0][0], result.result_rows[0][1]
    # No watermark yet — start from the beginning of time
    return datetime(1970, 1, 1), ""


def update_watermark(ch, timestamp: datetime, span_id: str):
    """Update the enricher watermark."""
    ch.insert(
        "enricher_watermark",
        [["global", timestamp, span_id, datetime.utcnow()]],
        column_names=["WatermarkKey", "LastTimestamp", "LastSpanId", "UpdatedAt"],
    )


def build_embedding_text(row: dict) -> str:
    """
    Build a human-readable text representation of a span for embedding.
    Combines service name, span name, status, duration, and key attributes.
    """
    parts = [
        f"service={row['ServiceName']}",
        f"span={row['SpanName']}",
        f"kind={row['SpanKind']}",
        f"status={row['StatusCode']}",
    ]

    duration_ms = row["Duration"] / 1_000_000  # ns → ms
    parts.append(f"duration={duration_ms:.1f}ms")

    if row["StatusMessage"]:
        parts.append(f"message={row['StatusMessage']}")

    # Include span attributes
    for k, v in row.get("SpanAttributes", {}).items():
        parts.append(f"{k}={v}")

    return " ".join(parts)


FETCH_QUERY = """
SELECT
    Timestamp, TraceId, SpanId, ParentSpanId,
    SpanName, SpanKind, ServiceName,
    Duration, StatusCode, StatusMessage,
    ResourceAttributes, SpanAttributes
FROM otel_traces
WHERE (Timestamp, SpanId) > ({ts:DateTime64(9)}, {span_id:String})
ORDER BY Timestamp, SpanId
LIMIT {limit:UInt32}
"""


ENRICHED_COLUMNS = [
    "Timestamp", "TraceId", "SpanId", "ParentSpanId",
    "SpanName", "SpanKind", "ServiceName",
    "Duration", "StatusCode", "StatusMessage",
    "ResourceAttributesFlat", "SpanAttributesFlat",
    "EmbeddingText", "Embedding",
]


def fetch_new_rows(ch, watermark_ts: datetime, watermark_span_id: str) -> list[dict]:
    """Fetch rows from otel_traces that are past the watermark."""
    result = ch.query(
        FETCH_QUERY,
        parameters={
            "ts": watermark_ts,
            "span_id": watermark_span_id,
            "limit": config.BATCH_SIZE,
        },
    )

    columns = result.column_names
    rows = []
    for row_data in result.result_rows:
        row = dict(zip(columns, row_data))
        rows.append(row)
    return rows


def _rows_to_columns(rows: list[list], n_cols: int) -> list[list]:
    """Transpose row-oriented data to columnar format."""
    return [[row[i] for row in rows] for i in range(n_cols)]


def enrich_and_insert(ch, model: SentenceTransformer, rows: list[dict]):
    """Compute embeddings and insert enriched rows using columnar format."""
    # Build embedding texts
    texts = [build_embedding_text(row) for row in rows]

    # Batch encode with sub-batching for GPU memory management
    embeddings = model.encode(texts, show_progress_bar=False, convert_to_numpy=True,
                              batch_size=512)

    # Build insert data as rows first, then convert to columnar
    insert_rows = []
    for row, text, embedding in zip(rows, texts, embeddings):
        insert_rows.append([
            row["Timestamp"],
            row["TraceId"],
            row["SpanId"],
            row["ParentSpanId"],
            row["SpanName"],
            row["SpanKind"],
            row["ServiceName"],
            row["Duration"],
            row["StatusCode"],
            row["StatusMessage"],
            row.get("ResourceAttributes", {}),
            row.get("SpanAttributes", {}),
            text,
            embedding.tolist(),
        ])

    col_data = _rows_to_columns(insert_rows, len(ENRICHED_COLUMNS))
    ch.insert(
        "otel_traces_enriched",
        col_data,
        column_names=ENRICHED_COLUMNS,
        column_oriented=True,
    )


# ── Prefetch thread ───────────────────────────────────────────────

def prefetch_loop(prefetch_queue: queue.Queue, stop_event: threading.Event):
    """Continuously fetch the next batch of rows while GPU is encoding."""
    ch = get_ch_client()

    while not stop_event.is_set():
        try:
            watermark_ts, watermark_span_id = get_watermark(ch)
            rows = fetch_new_rows(ch, watermark_ts, watermark_span_id)

            if rows:
                # Block until the main thread consumes the previous batch
                prefetch_queue.put(rows)
            else:
                time.sleep(config.POLL_INTERVAL)

        except Exception as e:
            log.error("Prefetch error: %s", e, exc_info=True)
            time.sleep(config.POLL_INTERVAL)


def run():
    log.info("Embedding Enricher starting")
    log.info("  CH host:     %s", config.CH_HOST)
    log.info("  Model:       %s", config.MODEL_NAME)
    log.info("  Batch size:  %d", config.BATCH_SIZE)
    log.info("  Poll interval: %ds", config.POLL_INTERVAL)

    log.info("Loading model %s ...", config.MODEL_NAME)
    model = SentenceTransformer(config.MODEL_NAME)
    log.info("Model loaded")

    ch = get_ch_client()

    # Start prefetch thread
    prefetch_q = queue.Queue(maxsize=2)  # at most 2 batches buffered
    stop_event = threading.Event()
    prefetch_thread = threading.Thread(
        target=prefetch_loop, args=(prefetch_q, stop_event),
        daemon=True, name="prefetch",
    )
    prefetch_thread.start()
    log.info("Prefetch thread started")

    try:
        while True:
            try:
                # Wait for prefetched batch (with timeout so we can check for shutdown)
                rows = prefetch_q.get(timeout=config.POLL_INTERVAL)
            except queue.Empty:
                continue

            try:
                log.info(
                    "Enriching %d rows",
                    len(rows),
                )

                enrich_and_insert(ch, model, rows)

                # Update watermark to the last row processed
                last = rows[-1]
                update_watermark(ch, last["Timestamp"], last["SpanId"])

                log.info("  → %d rows enriched and inserted", len(rows))
            except Exception as e:
                log.error("Enrichment cycle error: %s", e, exc_info=True)

    finally:
        stop_event.set()


if __name__ == "__main__":
    run()
