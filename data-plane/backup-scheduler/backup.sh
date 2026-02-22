#!/bin/bash
set -euo pipefail

# ── Config ────────────────────────────────────────────────────
CH_HOST="${CH_HOST:-clickhouse}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-admin}"
CH_PASSWORD="${CH_PASSWORD:-clickhouse123}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
S3_BACKUP_BUCKET="${S3_BACKUP_BUCKET:-ch-backups}"
BACKUP_INTERVAL_SECONDS="${BACKUP_INTERVAL_SECONDS:-3600}"

TABLES="otel_traces otel_logs otel_metrics"

ch_query() {
    clickhouse-client \
        --host "$CH_HOST" \
        --port "$CH_PORT" \
        --user "$CH_USER" \
        --password "$CH_PASSWORD" \
        --database otel \
        --query "$1"
}

# Configure MinIO client
mc alias set s3 "$S3_ENDPOINT" "$S3_ACCESS_KEY" "$S3_SECRET_KEY" 2>/dev/null

echo "Backup scheduler started (interval=${BACKUP_INTERVAL_SECONDS}s)"

while true; do
    echo "──── Backup run started at $(date -Iseconds) ────"

    # Collect metadata for metadata.json
    ALL_SERVICES=""
    declare -A TABLE_PARTITIONS

    for TABLE in $TABLES; do
        echo "Processing $TABLE ..."

        # Get active partition IDs from system.parts
        ACTIVE_PARTS=$(ch_query \
            "SELECT DISTINCT partition_id FROM system.parts
             WHERE database = 'otel' AND table = '$TABLE' AND active
             ORDER BY partition_id" 2>/dev/null || true)

        if [ -z "$ACTIVE_PARTS" ]; then
            echo "  No active partitions for $TABLE"
            TABLE_PARTITIONS[$TABLE]=""
            continue
        fi

        # Get already-backed-up partitions from watermark
        DONE_PARTS=$(ch_query \
            "SELECT PartitionId FROM otel.backup_watermark FINAL
             WHERE TableName = '$TABLE' AND Status = 'done'" 2>/dev/null || true)

        BACKED_UP=()
        for PART in $ACTIVE_PARTS; do
            # Skip if already backed up
            if echo "$DONE_PARTS" | grep -qxF "$PART"; then
                BACKED_UP+=("$PART")
                continue
            fi

            S3_PATH="${S3_ENDPOINT}/${S3_BACKUP_BUCKET}/${TABLE}/${PART}/"
            echo "  Backing up partition $PART → $S3_PATH"

            if ch_query \
                "BACKUP TABLE otel.${TABLE} PARTITIONS '${PART}'
                 TO S3('${S3_PATH}', '${S3_ACCESS_KEY}', '${S3_SECRET_KEY}')" \
                2>/dev/null; then
                # Record success
                ch_query \
                    "INSERT INTO otel.backup_watermark (TableName, PartitionId, Status)
                     VALUES ('${TABLE}', '${PART}', 'done')" 2>/dev/null
                echo "  ✓ $TABLE/$PART backed up"
                BACKED_UP+=("$PART")
            else
                # Record failure
                ch_query \
                    "INSERT INTO otel.backup_watermark (TableName, PartitionId, Status)
                     VALUES ('${TABLE}', '${PART}', 'failed')" 2>/dev/null
                echo "  ✗ $TABLE/$PART failed"
            fi
        done

        # Build partition list for metadata (all successfully backed up)
        TABLE_PARTITIONS[$TABLE]=$(printf '%s\n' "${BACKED_UP[@]}" | sort -u | tr '\n' ',' | sed 's/,$//')

        # Collect services from traces
        if [ "$TABLE" = "otel_traces" ]; then
            ALL_SERVICES=$(ch_query \
                "SELECT DISTINCT ServiceName FROM otel.otel_traces ORDER BY ServiceName" \
                2>/dev/null || true)
        fi
    done

    # ── Write metadata.json to S3 ────────────────────────────
    SERVICES_JSON=$(echo "$ALL_SERVICES" | awk 'NF{printf "%s\"%s\"", (NR>1?",":""), $0}')

    TABLES_JSON=""
    for TABLE in $TABLES; do
        PARTS="${TABLE_PARTITIONS[$TABLE]:-}"
        PARTS_JSON=""
        if [ -n "$PARTS" ]; then
            PARTS_JSON=$(echo "$PARTS" | tr ',' '\n' | awk 'NF{printf "%s\"%s\"", (NR>1?",":""), $0}')
        fi
        if [ -n "$TABLES_JSON" ]; then
            TABLES_JSON="${TABLES_JSON},"
        fi
        TABLES_JSON="${TABLES_JSON}\"${TABLE}\":[${PARTS_JSON}]"
    done

    METADATA="{\"services\":[${SERVICES_JSON}],\"tables\":{${TABLES_JSON}}}"

    echo "$METADATA" | mc pipe "s3/${S3_BACKUP_BUCKET}/metadata.json" 2>/dev/null \
        && echo "metadata.json uploaded" \
        || echo "Failed to upload metadata.json"

    echo "──── Backup run complete ────"
    echo "Sleeping ${BACKUP_INTERVAL_SECONDS}s ..."
    sleep "$BACKUP_INTERVAL_SECONDS"
done
