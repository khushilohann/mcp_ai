import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app as main_app
from app.db.seed import seed as seed_db


@pytest.mark.asyncio
async def test_query_data_executes_lle_and_sql(monkeypatch):
    # Seed the DB to ensure data exists
    await seed_db()

    transport = ASGITransport(app=main_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # LLM mock will return a SELECT query for "show users"
        r = await ac.post("/mcp/query_data", json={"question":"show users"})
        assert r.status_code == 200
        body = r.json()
        assert body.get("success") is True
        assert "generated_sql" in body
        assert body.get("execution", {}).get("success") is True
        rows = body.get("execution", {}).get("rows")
        assert isinstance(rows, list)
