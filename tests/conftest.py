import asyncio
from typing import Callable

import pytest
import pytest_asyncio

from qstash import QStash, AsyncQStash
from tests import QSTASH_TOKEN


@pytest.fixture
def client():
    return QStash(token=QSTASH_TOKEN)


@pytest_asyncio.fixture
async def async_client():
    return AsyncQStash(token=QSTASH_TOKEN)


@pytest.fixture
def cleanup_queue(request: pytest.FixtureRequest) -> Callable[[QStash, str], None]:
    queue_names = []

    def register(client: QStash, queue_name: str) -> None:
        queue_names.append((client, queue_name))

    def delete():
        for client, queue_name in queue_names:
            try:
                client.queue.delete(queue_name)
            except Exception:
                pass

    request.addfinalizer(delete)

    return register


@pytest_asyncio.fixture
def cleanup_queue_async(
    request: pytest.FixtureRequest,
) -> Callable[[AsyncQStash, str], None]:
    queue_names = []

    def register(async_client: AsyncQStash, queue_name: str) -> None:
        queue_names.append((async_client, queue_name))

    def finalizer():
        async def delete():
            for async_client, queue_name in queue_names:
                try:
                    await async_client.queue.delete(queue_name)
                except Exception:
                    pass

        asyncio.run(delete())

    request.addfinalizer(finalizer)

    return register


@pytest.fixture
def cleanup_schedule(request: pytest.FixtureRequest) -> Callable[[QStash, str], None]:
    schedule_ids = []

    def register(client: QStash, schedule_id: str) -> None:
        schedule_ids.append((client, schedule_id))

    def delete():
        for client, schedule_id in schedule_ids:
            try:
                client.schedule.delete(schedule_id)
            except Exception:
                pass

    request.addfinalizer(delete)

    return register


@pytest_asyncio.fixture
def cleanup_schedule_async(
    request: pytest.FixtureRequest,
) -> Callable[[AsyncQStash, str], None]:
    schedule_ids = []

    def register(async_client: AsyncQStash, schedule_id: str) -> None:
        schedule_ids.append((async_client, schedule_id))

    def finalizer():
        async def delete():
            for async_client, schedule_id in schedule_ids:
                try:
                    await async_client.schedule.delete(schedule_id)
                except Exception:
                    pass

        asyncio.run(delete())

    request.addfinalizer(finalizer)

    return register
