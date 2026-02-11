import pytest

from qstash import AsyncQStash


@pytest.mark.asyncio
async def test_check_async(async_client: AsyncQStash) -> None:
    response = await async_client.readiness.check()
    assert response.ready is True
