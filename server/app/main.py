import os
import time
import traceback
from uuid import uuid4

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from app.tools.query_data import router as query_router
from app.tools.list_sources import router as sources_router
from app.tools.transform_data import router as transform_router
from app.tools.export_data import router as export_router
from app.tools.query_api import router as query_api_router
from app.core.logging import setup_logging, get_logger
from app.core.ollama_client import ask_ollama

setup_logging()
logger = get_logger("mcp.server")

app = FastAPI(title="MCP Server with Ollama")

app.include_router(query_router, prefix="/mcp")
app.include_router(sources_router, prefix="/mcp")
app.include_router(transform_router, prefix="/mcp")
app.include_router(export_router, prefix="/mcp")
app.include_router(query_api_router, prefix="/mcp")


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
