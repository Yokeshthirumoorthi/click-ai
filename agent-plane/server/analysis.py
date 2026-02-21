"""Analysis engine — LLM generates ClickHouse SQL, executes via chDB (embedded)."""

import json
import logging

import chdb
from fastapi import APIRouter, Depends, HTTPException
from openai import OpenAI
from pydantic import BaseModel

from . import config
from .auth import get_current_user
from .formatter import format_results
from .sessions import _sessions
from .vector_search import search as vector_search

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sessions", tags=["analysis"])

SYSTEM_PROMPT = """You are a data analyst working with a ClickHouse database containing OpenTelemetry observability data.

Given the user's question and the database schema below, generate a ClickHouse SQL query to answer it.

RULES:
- Return ONLY a JSON object with keys "sql" and "explanation"
- "sql" is the ClickHouse SQL query string
- "explanation" is a brief description of what the query does
- Use ClickHouse SQL syntax
- Use the `default` database — qualify tables as `default`.table_name
- Timestamps are DateTime64(9) — use toDateTime(), toDate(), formatDateTime() etc.
- Duration is UInt64 in nanoseconds (divide by 1000000 for milliseconds)
- SpanAttributes and ResourceAttributes are Map(LowCardinality(String), String)
  - Access keys: SpanAttributes['key_name']
  - List all keys: mapKeys(SpanAttributes)
  - List all values: mapValues(SpanAttributes)
  - Check if key exists: mapContains(SpanAttributes, 'key_name')
- LogAttributes is also a Map column — same access pattern
- MetricAttributes is also a Map column — same access pattern
- For string matching use: like, notLike, match (regex), extract
- For aggregation: use ClickHouse functions (quantile, uniq, groupArray, etc.)
- Do NOT use any DML statements (INSERT, UPDATE, DELETE, DROP, etc.)
- Keep queries efficient — use LIMIT when exploring

DATABASE SCHEMA:
{schema}"""


def _llm_client() -> OpenAI:
    return OpenAI(
        api_key=config.OPENROUTER_API_KEY,
        base_url=config.OPENROUTER_BASE_URL,
    )


def _schema_from_manifest(manifest: dict) -> str:
    parts = []
    for table_name, info in manifest.items():
        cols = ", ".join(f"{c['name']} {c['type']}" for c in info["columns"])
        parts.append(f"TABLE {table_name} ({cols}) — {info['row_count']} rows")
        if info.get("sample_rows"):
            parts.append(f"  Sample: {json.dumps(info['sample_rows'][0], default=str)[:300]}")
    return "\n".join(parts)


def _execute_sql(session_id: str, sql: str) -> tuple[list[str], list[dict]]:
    """Execute a read-only SQL query via chDB session."""
    session_path = str(config.SESSION_DIR / session_id)
    sess = chdb.Session(session_path)
    result = sess.query(sql, "JSON")
    parsed = json.loads(result.bytes())
    rows = parsed.get("data", [])
    columns = list(rows[0].keys()) if rows else []
    return columns, rows


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    question: str
    sql: str
    explanation: str
    formatted: str
    row_count: int


@router.post("/{session_id}/ask", response_model=AskResponse)
def ask(
    session_id: str,
    req: AskRequest,
    user: str = Depends(get_current_user),
):
    session = _sessions.get(session_id)
    if not session or session["user"] != user:
        raise HTTPException(404, "Session not found")
    if session["status"] != "ready":
        raise HTTPException(400, f"Session is not ready (status: {session['status']})")

    manifest = session.get("manifest", {})
    if not manifest:
        raise HTTPException(400, "Session has no data")

    # C8: vector search (placeholder — returns empty)
    vector_results = vector_search(session_id, req.question)

    # Build conversation with history
    schema_text = _schema_from_manifest(manifest)
    system_msg = SYSTEM_PROMPT.format(schema=schema_text)

    messages = [{"role": "system", "content": system_msg}]

    # Add conversation history (last 10 exchanges)
    for entry in session.get("conversation", [])[-10:]:
        messages.append({"role": "user", "content": entry["question"]})
        messages.append({"role": "assistant", "content": json.dumps({"sql": entry["sql"], "explanation": entry["explanation"]})})

    messages.append({"role": "user", "content": req.question})

    # Call LLM to generate SQL
    client = _llm_client()
    resp = client.chat.completions.create(
        model=config.LLM_MODEL,
        messages=messages,
        max_tokens=1024,
    )

    raw = resp.choices[0].message.content.strip()

    # Parse JSON from response (handle markdown code blocks)
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        parsed = json.loads(raw)
        sql = parsed["sql"]
        explanation = parsed.get("explanation", "")
    except (json.JSONDecodeError, KeyError):
        raise HTTPException(502, f"LLM returned invalid response: {raw[:200]}")

    # Safety check
    sql_upper = sql.strip().upper()
    if any(sql_upper.startswith(kw) for kw in ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"]):
        raise HTTPException(400, "Write operations are not allowed")

    # Execute via chDB
    try:
        columns, rows = _execute_sql(session_id, sql)
    except Exception as e:
        raise HTTPException(400, f"SQL execution failed: {e}")

    # C10: format results
    formatted = format_results(req.question, sql, rows, columns)

    # Save to conversation history
    session.setdefault("conversation", []).append({
        "question": req.question,
        "sql": sql,
        "explanation": explanation,
        "row_count": len(rows),
    })

    return AskResponse(
        question=req.question,
        sql=sql,
        explanation=explanation,
        formatted=formatted,
        row_count=len(rows),
    )


@router.get("/{session_id}/history")
def get_history(session_id: str, user: str = Depends(get_current_user)):
    session = _sessions.get(session_id)
    if not session or session["user"] != user:
        raise HTTPException(404, "Session not found")
    return {"history": session.get("conversation", [])}
