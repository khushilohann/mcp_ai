from fastapi import FastAPI
from app.tools.query_data import router as query_router
from app.tools.list_sources import router as sources_router

app = FastAPI(title="MCP Server with Ollama")

app.include_router(query_router, prefix="/mcp")
app.include_router(sources_router, prefix="/mcp")

@app.get("/")
async def root():
    return {"status": "MCP Server Running with Ollama"}
