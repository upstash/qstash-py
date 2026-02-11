import pytest

from qstash import AsyncQStash


@pytest.mark.asyncio
async def test_readiness_async(async_client: AsyncQStash) -> None:
    response = await async_client.readiness()
    assert response == "OK"
