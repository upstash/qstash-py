import pytest
from upstash_qstash import AsyncQStash


@pytest.mark.asyncio
async def test_queue_async(async_qstash: AsyncQStash) -> None:
    await async_qstash.queue.upsert(queue="test_queue", parallelism=1)

    queue = await async_qstash.queue.get("test_queue")
    assert queue.name == "test_queue"
    assert queue.parallelism == 1

    await async_qstash.queue.upsert(queue="test_queue", parallelism=2)

    queue = await async_qstash.queue.get("test_queue")
    assert queue.name == "test_queue"
    assert queue.parallelism == 2

    all_queues = await async_qstash.queue.list()
    assert any(True for q in all_queues if q.name == "test_queue")

    await async_qstash.queue.delete("test_queue")

    all_queues = await async_qstash.queue.list()
    assert not any(True for q in all_queues if q.name == "test_queue")
