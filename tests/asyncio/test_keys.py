import pytest
from upstash_qstash.asyncio import Client
from qstash_tokens import (
    QSTASH_TOKEN,
    QSTASH_CURRENT_SIGNING_KEY,
    QSTASH_NEXT_SIGNING_KEY,
)


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


@pytest.mark.asyncio
async def test_get_keys(client):
    keys = client.keys()
    both_keys = await keys.get()
    assert both_keys["current"] == QSTASH_CURRENT_SIGNING_KEY
    assert both_keys["next"] == QSTASH_NEXT_SIGNING_KEY
