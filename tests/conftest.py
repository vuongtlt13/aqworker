from typing import AsyncIterator, Mapping, Optional
from unittest.mock import patch

import fakeredis.aioredis
import pytest
import pytest_asyncio
from redis import asyncio as redis


@pytest.fixture
def env(monkeypatch):
    """
    Helper fixture that lets tests override environment variables in a scoped way.
    Accepts either a mapping or keyword pairs and applies them via monkeypatch.
    """

    def _set_env(
        mapping: Optional[Mapping[str, str]] = None, **kwargs
    ) -> dict[str, str]:
        updates: dict[str, str] = {}
        if mapping:
            updates.update(dict(mapping))
        updates.update(kwargs)
        for key, value in updates.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, value)
        return updates

    return _set_env


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[redis.Redis]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def mock_aioredis_from_url(redis_client):
    with patch("redis.asyncio.from_url", return_value=redis_client):
        yield
