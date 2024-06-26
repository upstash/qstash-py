from typing import Callable

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
        url="https://httpstat.us/200",
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
        url="https://httpstat.us/200",
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
            url="https://httpstat.us/200",
            url_group="test-url-group",
        )

    with pytest.raises(QStashError):
        await async_qstash.message.publish_json(
            url="https://httpstat.us/200",
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
                url="https://httpstat.us/200",
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
                url="https://httpstat.us/200",
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
        callback="https://httpstat.us/200",
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
                "callback": "https://httpstat.us/200",
            }
        ]
    )

    assert len(res) == 1

    assert isinstance(res[0], BatchResponse)
    assert len(res[0].message_id) > 0

    await assert_delivered_eventually_async(async_qstash, res[0].message_id)


@pytest.mark.asyncio
async def test_enqueue_async(
    async_qstash: AsyncQStash,
    cleanup_queue_async: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue_async(async_qstash, name)

    res = await async_qstash.message.enqueue(
        queue=name,
        body="test-body",
        url="https://httpstat.us/200",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0


@pytest.mark.asyncio
async def test_enqueue_json_async(
    async_qstash: AsyncQStash,
    cleanup_queue_async: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue_async(async_qstash, name)

    res = await async_qstash.message.enqueue_json(
        queue=name,
        body={"test": "body"},
        url="https://httpstat.us/200",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0


@pytest.mark.asyncio
async def test_enqueue_api_llm_async(
    async_qstash: AsyncQStash,
    cleanup_queue_async: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue_async(async_qstash, name)

    res = await async_qstash.message.enqueue_json(
        queue=name,
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
        callback="https://httpstat.us/200",
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0


@pytest.mark.asyncio
async def test_publish_to_url_group_async(async_qstash: AsyncQStash) -> None:
    name = "python_url_group"
    await async_qstash.url_group.delete(name)

    await async_qstash.url_group.upsert_endpoints(
        url_group=name,
        endpoints=[
            {"url": "https://httpstat.us/200"},
            {"url": "https://httpstat.us/201"},
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


@pytest.mark.asyncio
async def test_timeout_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.publish_json(
        body={"ex_key": "ex_value"},
        url="https://httpstat.us/200",
        timeout=90,
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_qstash, res.message_id)


@pytest.mark.asyncio
async def test_cancel_many_async(async_qstash: AsyncQStash) -> None:
    res0 = await async_qstash.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res0, PublishResponse)

    res1 = await async_qstash.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res1, PublishResponse)

    cancelled = await async_qstash.message.cancel_many(
        [res0.message_id, res1.message_id]
    )

    assert cancelled == 2


@pytest.mark.asyncio
async def test_cancel_all_async(async_qstash: AsyncQStash) -> None:
    res0 = await async_qstash.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res0, PublishResponse)

    res1 = await async_qstash.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res1, PublishResponse)

    cancelled = await async_qstash.message.cancel_all()

    assert cancelled >= 2
