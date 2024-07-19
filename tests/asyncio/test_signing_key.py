import pytest

from qstash import AsyncQStash
from tests import QSTASH_CURRENT_SIGNING_KEY, QSTASH_NEXT_SIGNING_KEY


@pytest.mark.asyncio
async def test_get_async(async_client: AsyncQStash) -> None:
    key = await async_client.signing_key.get()
    assert key.current == QSTASH_CURRENT_SIGNING_KEY
    assert key.next == QSTASH_NEXT_SIGNING_KEY
