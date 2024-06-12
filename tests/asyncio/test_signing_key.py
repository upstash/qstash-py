import pytest
from tests import QSTASH_CURRENT_SIGNING_KEY, QSTASH_NEXT_SIGNING_KEY
from upstash_qstash import AsyncQStash


@pytest.mark.asyncio
async def test_get_async(async_qstash: AsyncQStash) -> None:
    key = await async_qstash.signing_key.get()
    assert key.current == QSTASH_CURRENT_SIGNING_KEY
    assert key.next == QSTASH_NEXT_SIGNING_KEY
