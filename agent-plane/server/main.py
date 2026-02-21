"""FastAPI application — mounts static files and includes all routers."""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .analysis import router as analysis_router
from .auth import router as auth_router
from .file_ingester import router as file_router
from .sessions import router as sessions_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Click-AI Analysis Platform", version="1.0.0")

app.include_router(auth_router)
app.include_router(sessions_router)
app.include_router(analysis_router)
app.include_router(file_router)


@app.get("/api/health")
def health():
    return {"status": "ok"}


static_dir = Path(__file__).parent.parent / "static"
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
