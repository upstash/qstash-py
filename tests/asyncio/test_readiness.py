from qstash import AsyncQStash


async def test_readiness(async_client: AsyncQStash) -> None:
    response = await async_client.readiness()

    assert response is not None
    assert isinstance(response.ready, bool)
    assert response.ready is True
