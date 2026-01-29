import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.api_connector import APIConnector

MOCK_API_URL = os.getenv("MOCK_API_URL", "http://127.0.0.1:9001")

router = APIRouter()

# Simple connector cache to reuse clients per base_url+api_key
_CONNECTORS: Dict[str, APIConnector] = {}


def get_connector(base_url: str, api_key: Optional[str] = None, cache_ttl: int = 60) -> APIConnector:
    key = f"{base_url}::{api_key}"
    if key not in _CONNECTORS:
        _CONNECTORS[key] = APIConnector(base_url=base_url, api_key=api_key, cache_ttl=cache_ttl)
    return _CONNECTORS[key]


class QueryAPIRequest(BaseModel):
    method: str = "GET"  # GET/POST/PUT/DELETE
    path: str = "/"
    params: Optional[Dict[str, Any]] = None
    json: Optional[Dict[str, Any]] = None
    use_cache: Optional[bool] = True
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    cache_ttl: Optional[int] = 60
    invalidate_cache: Optional[bool] = False


@router.post("/query_api")
async def query_api(req: QueryAPIRequest):
    base_url = req.base_url or MOCK_API_URL
    api_key = req.api_key

    connector = get_connector(base_url, api_key, cache_ttl=req.cache_ttl or 60)

    method = req.method.upper()
    try:
        if method == "GET":
            result = await connector.get(req.path, params=req.params, use_cache=bool(req.use_cache))
            status = 200
        elif method == "POST":
            result = await connector.post(req.path, json=req.json)
            status = 201 if isinstance(result, dict) else 200
            if req.invalidate_cache:
                connector.cache.clear()
        elif method == "PUT":
            result = await connector.put(req.path, json=req.json)
            status = 200
            if req.invalidate_cache:
                connector.cache.clear()
        elif method == "DELETE":
            result = await connector.delete(req.path)
            status = 200
            if req.invalidate_cache:
                connector.cache.clear()
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported method: {req.method}")

        return {"success": True, "method": method, "status": status, "data": result}

    except Exception as e:
        return {"success": False, "error": {"message": str(e)}}
