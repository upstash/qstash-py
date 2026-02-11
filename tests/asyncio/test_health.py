import pytest

from qstash import AsyncQStash


@pytest.mark.asyncio
async def test_liveness_async(async_client: AsyncQStash) -> None:
    result = await async_client.liveness()
    assert result == "OK"


@pytest.mark.asyncio
async def test_readiness_async(async_client: AsyncQStash) -> None:
    result = await async_client.readiness()
    assert result == "OK"
