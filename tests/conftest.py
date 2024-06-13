import asyncio
from typing import Callable

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


@pytest.fixture
def cleanup_queue(request: pytest.FixtureRequest) -> Callable[[QStash, str], None]:
    queue_names = []

    def register(qstash: QStash, queue_name: str) -> None:
        queue_names.append((qstash, queue_name))

    def delete():
        for qstash, queue_name in queue_names:
            try:
                qstash.queue.delete(queue_name)
            except Exception:
                pass

    request.addfinalizer(delete)

    return register


@pytest_asyncio.fixture
def cleanup_queue_async(
    request: pytest.FixtureRequest,
) -> Callable[[AsyncQStash, str], None]:
    queue_names = []

    def register(async_qstash: AsyncQStash, queue_name: str) -> None:
        queue_names.append((async_qstash, queue_name))

    def finalizer():
        async def delete():
            for async_qstash, queue_name in queue_names:
                try:
                    await async_qstash.queue.delete(queue_name)
                except Exception:
                    pass

        asyncio.run(delete())

    request.addfinalizer(finalizer)

    return register


@pytest.fixture
def cleanup_schedule(request: pytest.FixtureRequest) -> Callable[[QStash, str], None]:
    schedule_ids = []

    def register(qstash: QStash, schedule_id: str) -> None:
        schedule_ids.append((qstash, schedule_id))

    def delete():
        for qstash, schedule_id in schedule_ids:
            try:
                qstash.schedule.delete(schedule_id)
            except Exception:
                pass

    request.addfinalizer(delete)

    return register


@pytest_asyncio.fixture
def cleanup_schedule_async(
    request: pytest.FixtureRequest,
) -> Callable[[AsyncQStash, str], None]:
    schedule_ids = []

    def register(async_qstash: AsyncQStash, schedule_id: str) -> None:
        schedule_ids.append((async_qstash, schedule_id))

    def finalizer():
        async def delete():
            for async_qstash, schedule_id in schedule_ids:
                try:
                    await async_qstash.schedule.delete(schedule_id)
                except Exception:
                    pass

        asyncio.run(delete())

    request.addfinalizer(finalizer)

    return register
