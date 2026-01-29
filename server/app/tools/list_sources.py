from fastapi import APIRouter

router = APIRouter()

@router.get("/list_sources")
async def list_sources():
    return {
        "sources": [
            {"name": "SQLite Database", "type": "sql"},
            {"name": "REST API", "type": "api"},
            {"name": "CSV Files", "type": "file"}
        ]
    }
