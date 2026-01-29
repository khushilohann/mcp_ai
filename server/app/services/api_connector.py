import asyncio
import logging
from typing import Any, Dict, Optional

import httpx
from cachetools import TTLCache

logger = logging.getLogger(__name__)


class APIConnector:
    """Async REST API connector with API-key, OAuth2, TTL caching, and retries."""

    def __init__(self, base_url: str, api_key: Optional[str] = None, oauth2_token: Optional[str] = None, cache_ttl: int = 60, timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.oauth2_token = oauth2_token
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        if api_key:
            self.client.headers.update({"x-api-key": api_key})
        if oauth2_token:
            self.client.headers.update({"Authorization": f"Bearer {oauth2_token}"})
        self.cache = TTLCache(maxsize=1024, ttl=cache_ttl)
        self._lock = asyncio.Lock()

    def _cache_key(self, path: str, params: Optional[Dict[str, Any]] = None):
        if not params:
            return path
        # use a stable representation
        items = tuple(sorted(params.items()))
        return f"{path}?{items}"

    async def _request(self, method: str, path: str, oauth2_token: Optional[str] = None, **kwargs):
        url = path if path.startswith("http") else path
        # Set OAuth2 token if provided for this request
        headers = kwargs.pop("headers", {})
        if oauth2_token:
            headers["Authorization"] = f"Bearer {oauth2_token}"
        elif self.oauth2_token:
            headers["Authorization"] = f"Bearer {self.oauth2_token}"
        # retries
        last_exc = None
        for attempt in range(3):
            try:
                resp = await self.client.request(method, url, headers=headers, **kwargs)
                resp.raise_for_status()
                # Try JSON parse, fallback to text
                try:
                    return resp.json()
                except Exception:
                    return resp.text
            except Exception as e:
                last_exc = e
                wait = 0.5 * (2 ** attempt)
                logger.warning("API request failed on attempt %d: %s; retrying in %.1fs", attempt + 1, e, wait)
                await asyncio.sleep(wait)
        raise RuntimeError(f"API request failed after retries: {last_exc}")


    async def get(self, path: str, params: Optional[Dict[str, Any]] = None, use_cache: bool = True, oauth2_token: Optional[str] = None):
        key = self._cache_key(path, params)
        if use_cache and key in self.cache:
            logger.debug("Cache hit for %s", key)
            return self.cache[key]

        async with self._lock:
            # double-checked locking
            if use_cache and key in self.cache:
                return self.cache[key]
            result = await self._request("GET", path, params=params, oauth2_token=oauth2_token)
            if use_cache:
                self.cache[key] = result
            return result

    async def post(self, path: str, json: Optional[Dict[str, Any]] = None, oauth2_token: Optional[str] = None):
        return await self._request("POST", path, json=json, oauth2_token=oauth2_token)

    async def put(self, path: str, json: Optional[Dict[str, Any]] = None, oauth2_token: Optional[str] = None):
        return await self._request("PUT", path, json=json, oauth2_token=oauth2_token)

    async def delete(self, path: str, oauth2_token: Optional[str] = None):
        return await self._request("DELETE", path, oauth2_token=oauth2_token)

    async def close(self):
        await self.client.aclose()
