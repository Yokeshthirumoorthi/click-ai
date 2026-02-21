"""C10: Report formatter — turns raw query results into presentable reports."""

import json
import logging

from openai import OpenAI

from . import config

log = logging.getLogger(__name__)


def _llm_client() -> OpenAI:
    return OpenAI(
        api_key=config.OPENROUTER_API_KEY,
        base_url=config.OPENROUTER_BASE_URL,
    )


def format_results(question: str, sql: str, rows: list[dict], columns: list[str]) -> str:
    if not rows:
        return "The query returned no results."

    # For small result sets, let LLM summarize
    preview = json.dumps(rows[:50], default=str)
    client = _llm_client()
    resp = client.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a data analyst. The user asked a question and a SQL query was run. "
                    "Format the results into a clear, readable response. "
                    "Use markdown tables for tabular data. Highlight key findings. "
                    "Be concise but thorough."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Question: {question}\n\n"
                    f"SQL: {sql}\n\n"
                    f"Columns: {columns}\n\n"
                    f"Results ({len(rows)} rows, showing up to 50):\n{preview}"
                ),
            },
        ],
        max_tokens=2048,
    )
    return resp.choices[0].message.content
