from typing import Callable

import pytest

from qstash import AsyncQStash


@pytest.mark.asyncio
async def test_queue_async(
    async_client: AsyncQStash,
    cleanup_queue_async: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue_async(async_client, name)

    await async_client.queue.upsert(queue=name, parallelism=1)

    queue = await async_client.queue.get(name)
    assert queue.name == name
    assert queue.parallelism == 1

    await async_client.queue.upsert(queue=name, parallelism=2)

    queue = await async_client.queue.get(name)
    assert queue.name == name
    assert queue.parallelism == 2

    all_queues = await async_client.queue.list()
    assert any(True for q in all_queues if q.name == name)

    await async_client.queue.delete(name)

    all_queues = await async_client.queue.list()
    assert not any(True for q in all_queues if q.name == name)


@pytest.mark.asyncio
async def test_queue_pause_resume_async(
    async_client: AsyncQStash,
    cleanup_queue_async: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue_async(async_client, name)

    await async_client.queue.upsert(queue=name)

    queue = await async_client.queue.get(name)
    assert queue.paused is False

    await async_client.queue.pause(name)

    queue = await async_client.queue.get(name)
    assert queue.paused is True

    await async_client.queue.resume(name)

    queue = await async_client.queue.get(name)
    assert queue.paused is False

    await async_client.queue.upsert(name, paused=True)

    queue = await async_client.queue.get(name)
    assert queue.paused is True

    await async_client.queue.upsert(name, paused=False)

    queue = await async_client.queue.get(name)
    assert queue.paused is False
