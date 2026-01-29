import pytest
from httpx import AsyncClient, ASGITransport
from app.mock_api.main import app as mock_app


@pytest.mark.asyncio
async def test_health():
    transport = ASGITransport(app=mock_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/health")
        assert r.status_code == 200
        assert r.json().get("status") == "ok"


@pytest.mark.asyncio
async def test_crud_flow():
    # ensure clean store for the test
    from app.mock_api.main import reset_store
    reset_store()

    transport = ASGITransport(app=mock_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        headers = {"x-api-key": "demo-key"}
        # create
        r = await ac.post("/items", json={"name": "T1", "description": "d"}, headers=headers)
        assert r.status_code == 200
        item = r.json()
        assert item["id"] == 1

        # list
        r = await ac.get("/items", headers=headers)
        assert r.status_code == 200
        assert len(r.json()) == 1

        # get
        r = await ac.get("/items/1", headers=headers)
        assert r.status_code == 200

        # update
        r = await ac.put("/items/1", json={"name": "T1x", "description": "d2"}, headers=headers)
        assert r.status_code == 200
        assert r.json()["name"] == "T1x"

        # delete
        r = await ac.delete("/items/1", headers=headers)
        assert r.status_code == 200
        assert r.json().get("deleted") == 1
