import pytest

from tests import assert_eventually_async
from upstash_qstash import AsyncQStash
from upstash_qstash.errors import QStashError
from upstash_qstash.event import EventState
from upstash_qstash.message import (
    BatchJsonRequest,
    BatchRequest,
    BatchResponse,
    EnqueueResponse,
    PublishResponse,
)


async def assert_delivered_eventually_async(
    async_qstash: AsyncQStash, msg_id: str
) -> None:
    async def assertion() -> None:
        events = (
            await async_qstash.event.list(
                filter={
                    "message_id": msg_id,
                    "state": EventState.DELIVERED,
                }
            )
        ).events

        assert len(events) == 1

    await assert_eventually_async(
        assertion,
        initial_delay=1.0,
        retry_delay=1.0,
        timeout=10.0,
    )


@pytest.mark.asyncio
async def test_publish_to_url_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.publish(
        body="test-body",
        url="https://example.com",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_qstash, res.message_id)


@pytest.mark.asyncio
async def test_publish_to_url_json_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.publish_json(
        body={"ex_key": "ex_value"},
        url="https://example.com",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_qstash, res.message_id)


@pytest.mark.asyncio
async def test_disallow_multiple_destinations_async(async_qstash: AsyncQStash) -> None:
    with pytest.raises(QStashError):
        await async_qstash.message.publish_json(
            url="https://example.com",
            url_group="test-url-group",
        )

    with pytest.raises(QStashError):
        await async_qstash.message.publish_json(
            url="https://example.com",
            api="llm",
        )

    with pytest.raises(QStashError):
        await async_qstash.message.publish_json(
            url_group="test-url-group",
            api="llm",
        )


@pytest.mark.asyncio
async def test_batch_async(async_qstash: AsyncQStash) -> None:
    N = 3
    messages = []
    for i in range(N):
        messages.append(
            BatchRequest(
                body=f"hi {i}",
                url="https://example.com",
                retries=0,
                headers={
                    f"test-header-{i}": f"test-value-{i}",
                    "content-type": "text/plain",
                },
            )
        )

    res = await async_qstash.message.batch(messages)

    assert len(res) == N

    for r in res:
        assert isinstance(r, BatchResponse)
        assert len(r.message_id) > 0


@pytest.mark.asyncio
async def test_batch_json_async(async_qstash: AsyncQStash) -> None:
    N = 3
    messages = []
    for i in range(N):
        messages.append(
            BatchJsonRequest(
                body={"hi": i},
                url="https://example.com",
                retries=0,
                headers={
                    f"test-header-{i}": f"test-value-{i}",
                },
            )
        )

    res = await async_qstash.message.batch_json(messages)

    assert len(res) == N

    for r in res:
        assert isinstance(r, BatchResponse)
        assert len(r.message_id) > 0


@pytest.mark.asyncio
async def test_publish_to_api_llm_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.publish_json(
        api="llm",
        body={
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [
                {
                    "role": "user",
                    "content": "hello",
                }
            ],
        },
        callback="https://example.com",
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_qstash, res.message_id)


@pytest.mark.asyncio
async def test_batch_api_llm_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.batch_json(
        [
            {
                "api": "llm",
                "body": {
                    "model": "meta-llama/Meta-Llama-3-8B-Instruct",
                    "messages": [
                        {
                            "role": "user",
                            "content": "hello",
                        }
                    ],
                },
                "callback": "https://example.com",
            }
        ]
    )

    assert len(res) == 1

    assert isinstance(res[0], BatchResponse)
    assert len(res[0].message_id) > 0

    await assert_delivered_eventually_async(async_qstash, res[0].message_id)


@pytest.mark.asyncio
async def test_enqueue_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.enqueue(
        queue="test_queue",
        body="test-body",
        url="https://example.com",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0

    await async_qstash.queue.delete("test_queue")


@pytest.mark.asyncio
async def test_enqueue_json_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.enqueue_json(
        queue="test_queue",
        body={"test": "body"},
        url="https://example.com",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0

    await async_qstash.queue.delete("test_queue")


@pytest.mark.asyncio
async def test_enqueue_api_llm_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.enqueue_json(
        queue="test_queue",
        body={
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [
                {
                    "role": "user",
                    "content": "hello",
                }
            ],
        },
        api="llm",
        callback="https://example.com/",
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0

    await async_qstash.queue.delete("test_queue")


@pytest.mark.asyncio
async def test_publish_to_url_group_async(async_qstash: AsyncQStash) -> None:
    name = "python_url_group"
    await async_qstash.url_group.delete(name)

    await async_qstash.url_group.upsert_endpoints(
        url_group=name,
        endpoints=[
            {"url": "https://example.com"},
            {"url": "https://example.net"},
        ],
    )

    res = await async_qstash.message.publish(
        body="test-body",
        url_group=name,
    )

    assert isinstance(res, list)
    assert len(res) == 2

    await assert_delivered_eventually_async(async_qstash, res[0].message_id)
    await assert_delivered_eventually_async(async_qstash, res[1].message_id)
