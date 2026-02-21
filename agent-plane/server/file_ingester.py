"""C7: File ingester — PLACEHOLDER. Accepts uploads but does nothing yet."""

from fastapi import APIRouter, Depends, UploadFile

from .auth import get_current_user

router = APIRouter(prefix="/api/sessions", tags=["files"])


@router.post("/{session_id}/upload")
def upload_file(
    session_id: str,
    file: UploadFile,
    user: str = Depends(get_current_user),
):
    return {
        "status": "not_implemented",
        "message": "File upload is not implemented yet. This feature will allow you to join CSV/Excel files with production telemetry data.",
        "filename": file.filename,
    }
