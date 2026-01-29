



from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel


import os
import time
import traceback
from uuid import uuid4

from app.tools.query_data import router as query_router
from app.tools.list_sources import router as sources_router
from app.tools.transform_data import router as transform_router
from app.tools.export_data import router as export_router
from app.tools.query_api import router as query_api_router
from app.tools.file_connector import router as file_router
from app.tools.auth import router as auth_router
from app.tools.plugin_manager import router as plugin_router
from app.core.logging import setup_logging, get_logger
from app.core.ollama_client import ask_ollama
from app.core.audit import audit_middleware_factory



setup_logging()
logger = get_logger("mcp.server")


app = FastAPI(title="MCP Server with Ollama")
app.middleware('http')(audit_middleware_factory())

from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request/Response models for chat endpoint
class ChatRequest(BaseModel):
    query: str

class ChatResponse(BaseModel):
    response: str


import httpx
from app.tools.query_data import query_data, QueryPayload
from app.tools.query_api import query_api, QueryAPIRequest
from app.db.explorer import list_tables

# Integrate with DB, files, APIs, etc.
async def get_chatbot_response(query: str) -> str:
    # Route based on keywords in the query
    q = query.lower()
    if "api" in q:
        # Example user query: 'show me data from api path /users' or 'get user details from api path /users/20'
        import re
        m = re.search(r"api(?: path)? ([/\w\d_-]+)", q)
        api_path = m.group(1) if m else "/"
        req = QueryAPIRequest(method="GET", path=api_path, base_url=None)
        result = await query_api(req)
        if not result.get("success"):
            return f"API Error: {result.get('error', {}).get('message', 'Unknown error')}"
        data = result.get("data")
        if isinstance(data, dict) and data:
            return f"API Data: {data}"
        elif isinstance(data, list) and data:
            return f"API Data: {data[0]}"  # Show only the first item for brevity
        else:
            return "API call succeeded but returned no data."
    elif "sql" in q or "database" in q or "user" in q:
        # Example: call the /mcp/query_data endpoint
        payload = QueryPayload(question=query)
        result = await query_data(payload)
        if not result.get("success"):
            return f"SQL Error: {result.get('error', {}).get('message', 'Unknown error')}"
        rows = result.get("execution", {}).get("rows", [])
        if rows:
            # Try to find a user by name if mentioned
            import re
            m = re.search(r"user(?: with name)? ([\w\d_]+)", q)
            if m:
                username = m.group(1)
                filtered = [r for r in rows if str(r.get("name", "")).lower() == username.lower()]
                if filtered:
                    return f"User info: {filtered[0]}"
            # If query contains 'all users', return all users
            if "all users" in q:
                return f"All users: {rows}"
            # Otherwise, just show the first row
            return f"Result: {rows[0]}"
        else:
            return "No matching data found."
    elif "file" in q or "csv" in q or "table" in q:
        # Example: list tables in the DB
        tables = await list_tables()
        return f"Tables in DB: {tables}"
    else:
        return f"You asked: {query}\n(No matching handler found. Try mentioning 'api', 'sql', or 'file'.)"

@app.post("/chat", response_model=ChatResponse)
async def chat_endpoint(chat_request: ChatRequest = Body(...)):
    response = await get_chatbot_response(chat_request.query)
    return ChatResponse(response=response)

app.include_router(query_router, prefix="/mcp")
app.include_router(sources_router, prefix="/mcp")
app.include_router(transform_router, prefix="/mcp")
app.include_router(export_router, prefix="/mcp")
app.include_router(query_api_router, prefix="/mcp")
app.include_router(file_router, prefix="/mcp")
app.include_router(auth_router, prefix="/auth")
app.include_router(plugin_router, prefix="/plugins")


@app.middleware("http")
async def add_request_id_and_log(request: Request, call_next):
    request_id = str(uuid4())
    start = time.time()
    logger.info(f"request_start request_id={request_id} method={request.method} path={request.url.path}")
    try:
        response = await call_next(request)
    except Exception as exc:  # catch so we can log and re-raise handled by exception handlers
        logger.exception(f"unhandled error request_id={request_id} path={request.url.path} error={exc}")
        raise
    duration = (time.time() - start) * 1000
    logger.info(f"request_end request_id={request_id} method={request.method} path={request.url.path} status_code={response.status_code} duration_ms={duration:.1f}")
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": {"type": exc.__class__.__name__, "message": str(exc), "trace": traceback.format_exc()},
        },
    )


@app.get("/")
async def root():
    return {"status": "MCP Server Running with Ollama"}


@app.get("/health")
async def health():
    """Return overall service health; optionally check Ollama if configured."""
    result = {"status": "ok", "components": {}}

    # Ollama check (optional)
    mock_mode = os.getenv("OLLAMA_MOCK", "true").lower() in ("1", "true", "yes")
    if mock_mode:
        result["components"]["ollama"] = {"status": "mock"}
    else:
        try:
            resp = ask_ollama("health check")
            result["components"]["ollama"] = {"status": "ok", "response_sample": resp[:200]}
        except Exception as e:
            logger.exception("Ollama health check failed")
            result["components"]["ollama"] = {"status": "unavailable", "error": str(e)}
            result["status"] = "degraded"

    return result
