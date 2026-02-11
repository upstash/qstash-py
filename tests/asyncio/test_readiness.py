from qstash import AsyncQStash


async def test_readiness(async_client: AsyncQStash) -> None:
    response = await async_client.readiness()
    assert response is not None
    assert isinstance(response.success, bool)
    assert isinstance(response.message, str)
    assert isinstance(response.status_code, int)
    assert response.timestamp is not None
