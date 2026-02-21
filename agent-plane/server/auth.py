import hashlib
import hmac
import json
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from . import config

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _make_token(username: str) -> str:
    expires = int(time.time()) + config.TOKEN_EXPIRY_HOURS * 3600
    payload = json.dumps({"user": username, "exp": expires}, separators=(",", ":"))
    sig = hmac.new(config.AUTH_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def _verify_token(token: str) -> str:
    try:
        payload_str, sig = token.rsplit(".", 1)
    except ValueError:
        raise HTTPException(401, "Invalid token")
    expected = hmac.new(config.AUTH_SECRET.encode(), payload_str.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(401, "Invalid token")
    payload = json.loads(payload_str)
    if payload["exp"] < int(time.time()):
        raise HTTPException(401, "Token expired")
    return payload["user"]


def get_current_user(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing authorization header")
    return _verify_token(auth[7:])


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    username: str


@router.post("/login", response_model=LoginResponse)
def login(body: LoginRequest):
    if body.username != config.AUTH_USERNAME or body.password != config.AUTH_PASSWORD:
        raise HTTPException(401, "Invalid credentials")
    token = _make_token(body.username)
    return LoginResponse(token=token, username=body.username)
