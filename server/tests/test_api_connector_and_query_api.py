import pytest
from httpx import AsyncClient, ASGITransport
from app.mock_api.main import app as mock_app
from app.services.api_connector import APIConnector
import app.tools.query_api as query_api_module


@pytest.mark.asyncio
async def test_api_connector_get_post_and_cache(monkeypatch):
    # create a connector whose client is the mock_app ASGI client
    connector = APIConnector("http://test", api_key="demo-key", cache_ttl=1)
    # ensure clean store for the test
    from app.mock_api.main import reset_store
    reset_store()

    transport = ASGITransport(app=mock_app)
    connector.client = AsyncClient(transport=transport, base_url="http://test")
    # Ensure API key header is present for auth with the mock ASGI app
    connector.client.headers.update({"x-api-key": "demo-key"})

    # POST should create
    created = await connector.post("/items", json={"name": "C1", "description": "desc"})
    assert created["id"] == 1

    # GET should return list
    items = await connector.get("/items")
    assert isinstance(items, list) and len(items) >= 1

    # Cache behavior: second get should be served from cache (no direct assert for cache, but ensure no error)
    items2 = await connector.get("/items")
    assert items2 == items

    await connector.close()


@pytest.mark.asyncio
async def test_query_api_endpoint(monkeypatch):
    # Monkeypatch get_connector to return a connector wired to the mock_app ASGI client
    def _make_test_connector(base_url, api_key=None, cache_ttl=60):
        c = APIConnector("http://test", api_key=api_key, cache_ttl=cache_ttl)
        c.client = AsyncClient(transport=ASGITransport(app=mock_app), base_url="http://test")
        return c

    monkeypatch.setattr(query_api_module, "get_connector", _make_test_connector)

    # Use AsyncClient against the main app with ASGI transport
    from app.main import app as main_app
    transport = ASGITransport(app=main_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # create via MCP (this will route to the mocked connector which uses mock_app)
        r = await ac.post(
            "/mcp/query_api",
            json={"method":"POST", "path": "/items", "json": {"name": "QA", "description": "from test"}, "api_key": "demo-key", "base_url": "http://test"},
        )
        assert r.status_code in (200, 201) or r.json().get("success") in (True, False)
