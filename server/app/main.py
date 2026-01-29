

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
from app.tools.analyze_schema import router as analyze_schema_router
from app.tools.data_quality import router as data_quality_router
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
from app.services.sql_engine import execute_sql
from app.services.unified_search import search_everywhere_users


def format_as_table(data, title=None):
    """Format data (list of dicts or single dict) as a readable table."""
    if not data:
        return "No data available."
    
    # Handle single dict
    if isinstance(data, dict) and not isinstance(data, list):
        if "rows" in data:
            data = data["rows"]
        elif "data" in data:
            data = data["data"]
        else:
            # Single dict - convert to list
            data = [data]
    
    # Ensure it's a list
    if not isinstance(data, list) or len(data) == 0:
        return "No data available."
    
    # Get all unique keys from all rows
    all_keys = set()
    for row in data:
        if isinstance(row, dict):
            all_keys.update(row.keys())
    
    if not all_keys:
        return "No data available."
    
    columns = sorted(all_keys)
    
    # Calculate column widths
    col_widths = {}
    for col in columns:
        col_widths[col] = max(len(str(col)), max((len(str(row.get(col, ""))) for row in data if isinstance(row, dict)), default=0))
        # Cap at reasonable width
        col_widths[col] = min(col_widths[col], 30)
    
    # Build table
    lines = []
    if title:
        lines.append(title)
        lines.append("")
    
    # Header
    header = " | ".join(str(col).ljust(col_widths[col]) for col in columns)
    lines.append(header)
    lines.append("-" * len(header))
    
    # Rows
    for row in data[:100]:  # Limit to 100 rows for display
        if isinstance(row, dict):
            row_str = " | ".join(
                str(row.get(col, "")).ljust(col_widths[col])[:col_widths[col]]
                for col in columns
            )
            lines.append(row_str)
    
    if len(data) > 100:
        lines.append(f"\n... and {len(data) - 100} more rows")
    
    return "\n".join(lines)


# Integrate with DB, files, APIs, etc.
async def get_chatbot_response(query: str) -> str:
    # Route based on keywords in the query
    q = query.lower()
    # Unified search (users): supports id/email/name/region/signup_date with AND/OR,
    # searches SQL + REST API + local files and returns a single merged table.
    import re
    looks_like_user_search = (
        ("user" in q)
        or ("email" in q)
        or ("region" in q)
        or ("signup" in q)
        or ("signed up" in q)
        or ("name" in q)
        or (re.search(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", q) is not None)
        or (re.search(r"\b(id\s*(?:=|is)?\s*\d+)\b", q) is not None)
        or (re.search(r"\b(na|eu|apac|latam)\b", q) is not None)
        or (re.search(r"\b\d{4}-\d{2}-\d{2}\b", q) is not None)
    )

    if looks_like_user_search and "table" not in q:
        rows = await search_everywhere_users(query)
        if rows:
            return format_as_table(rows, "Search Results (SQL + API + Files):")
        # fall through if no rows found
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
            return format_as_table([data], "API Response:")
        elif isinstance(data, list) and data:
            return format_as_table(data, "API Response:")
        else:
            return "API call succeeded but returned no data."
    elif "sql" in q or "database" in q or "user" in q or "show" in q or "list" in q:
        import re

        # If the user explicitly asks for a specific user by name, do a targeted SQL query.
        # This avoids returning "all users" and avoids missing the user due to auto-LIMIT pagination.
        m = re.search(r"\buser\s*(?:with\s+name\s+)?([a-z0-9_]+)\b", q)
        if m and ("with name" in q or "name" in q):
            username = m.group(1).strip()
            exec_result = await execute_sql(
                "SELECT * FROM users WHERE lower(name) = lower(?)",
                params=(username,),
            )
            if not exec_result.get("success"):
                return f"SQL Error: {exec_result.get('error', 'Unknown error')}"
            rows = exec_result.get("rows", [])
            if rows:
                return format_as_table(rows, f"User info for '{username}':")
            return f"No user found with name '{username}'."

        # Otherwise fall back to the NL->SQL tool
        payload = QueryPayload(question=query)
        result = await query_data(payload)
        if not result.get("success"):
            return f"SQL Error: {result.get('error', {}).get('message', 'Unknown error')}"
        rows = result.get("execution", {}).get("rows", [])
        if rows:
            title = "Query Results:"
            if "all users" in q:
                title = "All Users:"
            elif "user" in q:
                title = "User Results:"
            return format_as_table(rows, title)
        return "No matching data found."
    elif "file" in q or "csv" in q or "table" in q:
        # Example: list tables in the DB
        tables = await list_tables()
        if tables:
            return format_as_table([{"table": t} for t in tables], "Tables in Database:")
        return "No tables found in the database."
    else:
        return f"You asked: {query}\n(No matching handler found. Try mentioning 'api', 'sql', 'database', 'user', or 'table'.)"

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
app.include_router(analyze_schema_router, prefix="/mcp")
app.include_router(data_quality_router, prefix="/mcp")
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
