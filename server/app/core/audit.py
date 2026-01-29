import os
import time
from fastapi import Request
from typing import Optional

def log_audit_event(event_type: str, user: Optional[str], detail: str):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    log_line = f"{timestamp} | {event_type} | user={user or '-'} | {detail}\n"
    log_path = os.getenv("AUDIT_LOG_PATH", "audit.log")
    with open(log_path, "a") as f:
        f.write(log_line)

def audit_middleware_factory():
    async def audit_middleware(request: Request, call_next):
        user = request.headers.get("x-username") or "anonymous"
        event_type = f"{request.method} {request.url.path}"
        body_bytes = await request.body()
        body_preview = body_bytes[:200] if body_bytes else b''
        log_audit_event(event_type, user, f"query={request.query_params} body={body_preview}")
        response = await call_next(request)
        return response
    return audit_middleware
