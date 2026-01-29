import os
from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import Dict

app = FastAPI(title="Mock REST API for MCP Demo")

# Simple in-memory store
_ITEMS: Dict[int, Dict] = {}
_NEXT_ID = 1
MOCK_API_KEY = os.getenv("MOCK_API_KEY", "demo-key")


def check_api_key(x_api_key: str = Header(None)):
    if x_api_key != MOCK_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True


class Item(BaseModel):
    id: int = None
    name: str
    description: str = None


@app.post("/items", dependencies=[Depends(check_api_key)])
async def create_item(item: Item):
    global _NEXT_ID
    item.id = _NEXT_ID
    _ITEMS[_NEXT_ID] = item.dict()
    _NEXT_ID += 1
    return _ITEMS[item.id]


@app.get("/items", dependencies=[Depends(check_api_key)])
async def list_items():
    return list(_ITEMS.values())


@app.get("/items/{item_id}", dependencies=[Depends(check_api_key)])
async def get_item(item_id: int):
    item = _ITEMS.get(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item


@app.put("/items/{item_id}", dependencies=[Depends(check_api_key)])
async def update_item(item_id: int, item: Item):
    if item_id not in _ITEMS:
        raise HTTPException(status_code=404, detail="Item not found")
    item.id = item_id
    _ITEMS[item_id] = item.dict()
    return _ITEMS[item_id]


@app.delete("/items/{item_id}", dependencies=[Depends(check_api_key)])
async def delete_item(item_id: int):
    if item_id not in _ITEMS:
        raise HTTPException(status_code=404, detail="Item not found")
    del _ITEMS[item_id]
    return {"deleted": item_id}


@app.get("/health")
async def health():
    return {"status": "ok", "items": len(_ITEMS)}


# Test helper: reset in-memory store to a clean state
def reset_store():
    """Clear internal store and reset ID counter (useful for tests)."""
    global _ITEMS, _NEXT_ID
    _ITEMS.clear()
    _NEXT_ID = 1
