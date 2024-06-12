import pytest
import pytest_asyncio

from tests import QSTASH_TOKEN
from upstash_qstash import QStash, AsyncQStash


@pytest.fixture
def qstash():
    return QStash(token=QSTASH_TOKEN)


@pytest_asyncio.fixture
async def async_qstash():
    return AsyncQStash(token=QSTASH_TOKEN)
