from fastapi import APIRouter
from app.db.session import DB_PATH
import os

MOCK_API_URL = os.getenv("MOCK_API_URL", "http://127.0.0.1:9001")

router = APIRouter()

@router.get("/list_sources")
async def list_sources():
    return {
        "sources": [
            {"name": "SQLite Database", "type": "sql", "path": DB_PATH},
            {"name": "REST API", "type": "api", "mock_url": MOCK_API_URL, "auth": {"header": "x-api-key", "sample_key": "demo-key"}},
            {"name": "CSV Files", "type": "file"}
        ]
    }
