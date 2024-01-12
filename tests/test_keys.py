import pytest
from upstash_qstash import Client
from qstash_tokens import (
    QSTASH_TOKEN,
    QSTASH_CURRENT_SIGNING_KEY,
    QSTASH_NEXT_SIGNING_KEY,
)


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


def test_get_keys(client):
    keys = client.keys()
    both_keys = keys.get()
    assert both_keys["current"] == QSTASH_CURRENT_SIGNING_KEY
    assert both_keys["next"] == QSTASH_NEXT_SIGNING_KEY
