"""C5: Session manager — CRUD for analysis sessions and their DuckDB files."""

import json
import logging
import uuid
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel

from . import config
from .auth import get_current_user
from .session_builder import build_session, get_available_services

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sessions", tags=["sessions"])

# In-memory session registry (sufficient for V1 single-server)
_sessions: dict[str, dict] = {}


class CreateSessionRequest(BaseModel):
    services: list[str] = []
    signal_types: list[str] = ["traces", "logs", "metrics"]
    start_time: datetime
    end_time: datetime


class SessionInfo(BaseModel):
    id: str
    status: str
    services: list[str]
    signal_types: list[str]
    start_time: str
    end_time: str
    created_at: str
    manifest: dict | None = None
    error: str | None = None


def _run_build(session_id: str, req: CreateSessionRequest):
    session = _sessions[session_id]
    db_path = config.SESSION_DIR / f"{session_id}.duckdb"
    try:
        result = build_session(
            db_path=db_path,
            services=req.services,
            signal_types=req.signal_types,
            start=req.start_time,
            end=req.end_time,
        )
        session["status"] = "ready"
        session["manifest"] = result["manifest"]
        session["counts"] = result["counts"]
        log.info("Session %s ready: %s", session_id, result["counts"])
    except Exception as e:
        session["status"] = "error"
        session["error"] = str(e)
        log.exception("Session %s build failed", session_id)


@router.post("", response_model=SessionInfo)
def create_session(
    req: CreateSessionRequest,
    bg: BackgroundTasks,
    user: str = Depends(get_current_user),
):
    session_id = uuid.uuid4().hex[:12]
    now = datetime.utcnow().isoformat()
    _sessions[session_id] = {
        "id": session_id,
        "user": user,
        "status": "building",
        "services": req.services,
        "signal_types": req.signal_types,
        "start_time": req.start_time.isoformat(),
        "end_time": req.end_time.isoformat(),
        "created_at": now,
        "manifest": None,
        "error": None,
        "conversation": [],
    }
    bg.add_task(_run_build, session_id, req)
    return SessionInfo(**{k: v for k, v in _sessions[session_id].items() if k != "conversation" and k != "user" and k != "counts"})


@router.get("", response_model=list[SessionInfo])
def list_sessions(user: str = Depends(get_current_user)):
    return [
        SessionInfo(**{k: v for k, v in s.items() if k != "conversation" and k != "user" and k != "counts"})
        for s in _sessions.values()
        if s["user"] == user
    ]


@router.get("/services")
def list_services(user: str = Depends(get_current_user)):
    try:
        return {"services": get_available_services()}
    except Exception as e:
        raise HTTPException(500, f"Failed to query ClickHouse: {e}")


@router.get("/{session_id}", response_model=SessionInfo)
def get_session(session_id: str, user: str = Depends(get_current_user)):
    session = _sessions.get(session_id)
    if not session or session["user"] != user:
        raise HTTPException(404, "Session not found")
    return SessionInfo(**{k: v for k, v in session.items() if k != "conversation" and k != "user" and k != "counts"})


@router.delete("/{session_id}")
def delete_session(session_id: str, user: str = Depends(get_current_user)):
    session = _sessions.get(session_id)
    if not session or session["user"] != user:
        raise HTTPException(404, "Session not found")
    db_path = config.SESSION_DIR / f"{session_id}.duckdb"
    db_path.unlink(missing_ok=True)
    # DuckDB may create .wal file
    wal_path = db_path.with_suffix(".duckdb.wal")
    wal_path.unlink(missing_ok=True)
    del _sessions[session_id]
    return {"status": "deleted"}
