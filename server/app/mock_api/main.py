import os
from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import Dict, List, Optional

app = FastAPI(title="Mock REST API for MCP Demo")

# Simple in-memory store
_ITEMS: Dict[int, Dict] = {}
_NEXT_ID = 1
MOCK_API_KEY = os.getenv("MOCK_API_KEY", "demo-key")

# Additional demo datasets
_USERS: Dict[int, Dict] = {}
_NEXT_USER_ID = 1


def check_api_key(x_api_key: str = Header(None)):
    if x_api_key != MOCK_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True


class Item(BaseModel):
    id: int = None
    name: str
    description: str = None


class User(BaseModel):
    id: int = None
    name: str
    email: str
    region: Optional[str] = None
    signup_date: Optional[str] = None  # YYYY-MM-DD


def seed_users(count: int = 60):
    """Seed a deterministic set of users so REST API has real searchable data."""
    global _USERS, _NEXT_USER_ID
    _USERS.clear()
    _NEXT_USER_ID = 1
    regions = ["NA", "EU", "APAC", "LATAM"]
    base = 1
    for i in range(1, count + 1):
        user = {
            "id": _NEXT_USER_ID,
            "name": f"ApiUser{i}",
            "email": f"apiuser{i}@example.com",
            "region": regions[i % len(regions)],
            "signup_date": f"2025-06-{(base + i):02d}" if (base + i) <= 28 else "2025-07-01",
        }
        _USERS[_NEXT_USER_ID] = user
        _NEXT_USER_ID += 1


seed_users()


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
    return {"status": "ok", "items": len(_ITEMS), "users": len(_USERS)}


# Test helper: reset in-memory store to a clean state
def reset_store():
    """Clear internal store and reset ID counter (useful for tests)."""
    global _ITEMS, _NEXT_ID, _USERS, _NEXT_USER_ID
    _ITEMS.clear()
    _NEXT_ID = 1
    seed_users()


@app.get("/users", dependencies=[Depends(check_api_key)])
async def list_users(
    id: Optional[int] = None,
    name: Optional[str] = None,
    email: Optional[str] = None,
    region: Optional[str] = None,
    signup_date: Optional[str] = None,
):
    """List users with optional filters (exact match)."""
    users = list(_USERS.values())
    if id is not None:
        users = [u for u in users if u.get("id") == id]
    if name:
        users = [u for u in users if str(u.get("name", "")).lower() == name.lower()]
    if email:
        users = [u for u in users if str(u.get("email", "")).lower() == email.lower()]
    if region:
        users = [u for u in users if str(u.get("region", "")).lower() == region.lower()]
    if signup_date:
        users = [u for u in users if str(u.get("signup_date", "")) == signup_date]
    return users


@app.get("/users/{user_id}", dependencies=[Depends(check_api_key)])
async def get_user(user_id: int):
    user = _USERS.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.post("/users", dependencies=[Depends(check_api_key)])
async def create_user(user: User):
    global _NEXT_USER_ID
    user.id = _NEXT_USER_ID
    _USERS[_NEXT_USER_ID] = user.dict()
    _NEXT_USER_ID += 1
    return _USERS[user.id]


@app.put("/users/{user_id}", dependencies=[Depends(check_api_key)])
async def update_user(user_id: int, user: User):
    if user_id not in _USERS:
        raise HTTPException(status_code=404, detail="User not found")
    user.id = user_id
    _USERS[user_id] = user.dict()
    return _USERS[user_id]


@app.delete("/users/{user_id}", dependencies=[Depends(check_api_key)])
async def delete_user(user_id: int):
    if user_id not in _USERS:
        raise HTTPException(status_code=404, detail="User not found")
    del _USERS[user_id]
    return {"deleted": user_id}