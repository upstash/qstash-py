from typing import Callable

import pytest

from qstash import AsyncQStash
from qstash.chat import upstash, openai
from qstash.errors import QStashError
from qstash.event import EventState
from qstash.message import (
    BatchJsonRequest,
    BatchRequest,
    BatchResponse,
    EnqueueResponse,
    PublishResponse,
    FlowControl
)
from tests import assert_eventually_async, OPENAI_API_KEY


async def assert_delivered_eventually_async(
    async_client: AsyncQStash, msg_id: str
) -> None:
    async def assertion() -> None:
        events = (
            await async_client.event.list(
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
        timeout=60.0,
    )


@pytest.mark.asyncio
async def test_publish_to_url_async(async_client: AsyncQStash) -> None:
    res = await async_client.message.publish(
        body="test-body",
        url="https://httpstat.us/200",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_client, res.message_id)


@pytest.mark.asyncio
async def test_publish_to_url_json_async(async_client: AsyncQStash) -> None:
    res = await async_client.message.publish_json(
        body={"ex_key": "ex_value"},
        url="https://httpstat.us/200",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_client, res.message_id)


@pytest.mark.asyncio
async def test_disallow_multiple_destinations_async(async_client: AsyncQStash) -> None:
    with pytest.raises(QStashError):
        await async_client.message.publish_json(
            url="https://httpstat.us/200",
            url_group="test-url-group",
        )

    with pytest.raises(QStashError):
        await async_client.message.publish_json(
            url="https://httpstat.us/200",
            api={"name": "llm", "provider": upstash()},
        )

    with pytest.raises(QStashError):
        await async_client.message.publish_json(
            url_group="test-url-group",
            api={"name": "llm", "provider": upstash()},
        )


@pytest.mark.asyncio
async def test_batch_async(async_client: AsyncQStash) -> None:
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

    res = await async_client.message.batch(messages)

    assert len(res) == N

    for r in res:
        assert isinstance(r, BatchResponse)
        assert len(r.message_id) > 0


@pytest.mark.asyncio
async def test_batch_json_async(async_client: AsyncQStash) -> None:
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

    res = await async_client.message.batch_json(messages)

    assert len(res) == N

    for r in res:
        assert isinstance(r, BatchResponse)
        assert len(r.message_id) > 0


@pytest.mark.asyncio
async def test_publish_to_api_llm_async(async_client: AsyncQStash) -> None:
    res = await async_client.message.publish_json(
        api={"name": "llm", "provider": upstash()},
        body={
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [
                {
                    "role": "user",
                    "content": "just say hello",
                }
            ],
        },
        callback="https://httpstat.us/200",
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_client, res.message_id)


@pytest.mark.asyncio
async def test_batch_api_llm_async(async_client: AsyncQStash) -> None:
    res = await async_client.message.batch_json(
        [
            {
                "api": {"name": "llm", "provider": upstash()},
                "body": {
                    "model": "meta-llama/Meta-Llama-3-8B-Instruct",
                    "messages": [
                        {
                            "role": "user",
                            "content": "just say hello",
                        }
                    ],
                },
                "callback": "https://httpstat.us/200",
            },
            {
                "api": {
                    "name": "llm",
                    "provider": openai(OPENAI_API_KEY),  # type:ignore[arg-type]
                },
                "body": {
                    "model": "gpt-3.5-turbo",
                    "messages": [
                        {
                            "role": "user",
                            "content": "just say hello",
                        }
                    ],
                },
                "callback": "https://httpstat.us/200",
            },
        ]
    )

    assert len(res) == 2

    assert isinstance(res[0], BatchResponse)
    assert len(res[0].message_id) > 0

    assert isinstance(res[1], BatchResponse)
    assert len(res[1].message_id) > 0

    await assert_delivered_eventually_async(async_client, res[0].message_id)
    await assert_delivered_eventually_async(async_client, res[1].message_id)


@pytest.mark.asyncio
async def test_enqueue_async(
    async_client: AsyncQStash,
    cleanup_queue_async: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue_async(async_client, name)

    res = await async_client.message.enqueue(
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
    async_client: AsyncQStash,
    cleanup_queue_async: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue_async(async_client, name)

    res = await async_client.message.enqueue_json(
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
    async_client: AsyncQStash,
    cleanup_queue_async: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue_async(async_client, name)

    res = await async_client.message.enqueue_json(
        queue=name,
        body={
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [
                {
                    "role": "user",
                    "content": "just say hello",
                }
            ],
        },
        api={"name": "llm", "provider": upstash()},
        callback="https://httpstat.us/200",
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0


@pytest.mark.asyncio
async def test_publish_to_url_group_async(async_client: AsyncQStash) -> None:
    name = "python_url_group"
    await async_client.url_group.delete(name)

    await async_client.url_group.upsert_endpoints(
        url_group=name,
        endpoints=[
            {"url": "https://httpstat.us/200"},
            {"url": "https://httpstat.us/201"},
        ],
    )

    res = await async_client.message.publish(
        body="test-body",
        url_group=name,
    )

    assert isinstance(res, list)
    assert len(res) == 2

    await assert_delivered_eventually_async(async_client, res[0].message_id)
    await assert_delivered_eventually_async(async_client, res[1].message_id)


@pytest.mark.asyncio
async def test_timeout_async(async_client: AsyncQStash) -> None:
    res = await async_client.message.publish_json(
        body={"ex_key": "ex_value"},
        url="https://httpstat.us/200",
        timeout=90,
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_client, res.message_id)


@pytest.mark.asyncio
async def test_cancel_many_async(async_client: AsyncQStash) -> None:
    res0 = await async_client.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res0, PublishResponse)

    res1 = await async_client.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res1, PublishResponse)

    cancelled = await async_client.message.cancel_many(
        [res0.message_id, res1.message_id]
    )

    assert cancelled == 2


@pytest.mark.asyncio
async def test_cancel_all_async(async_client: AsyncQStash) -> None:
    res0 = await async_client.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res0, PublishResponse)

    res1 = await async_client.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res1, PublishResponse)

    cancelled = await async_client.message.cancel_all()

    assert cancelled >= 2


@pytest.mark.asyncio
async def test_publish_to_api_llm_custom_provider_async(
    async_client: AsyncQStash,
) -> None:
    res = await async_client.message.publish_json(
        api={
            "name": "llm",
            "provider": openai(OPENAI_API_KEY),  # type:ignore[arg-type]
        },
        body={
            "model": "gpt-3.5-turbo",
            "messages": [
                {
                    "role": "user",
                    "content": "just say hello",
                }
            ],
        },
        callback="https://httpstat.us/200",
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_client, res.message_id)


@pytest.mark.asyncio
async def test_enqueue_api_llm_custom_provider_async(
    async_client: AsyncQStash,
    cleanup_queue: Callable[[AsyncQStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(async_client, name)

    res = await async_client.message.enqueue_json(
        queue=name,
        body={
            "model": "gpt-3.5-turbo",
            "messages": [
                {
                    "role": "user",
                    "content": "just say hello",
                }
            ],
        },
        api={
            "name": "llm",
            "provider": openai(OPENAI_API_KEY),  # type:ignore[arg-type]
        },
        callback="https://httpstat.us/200",
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0


@pytest.mark.asyncio
async def test_publish_with_flow_control_async(
    async_client: AsyncQStash,
) -> None:
    result = await async_client.message.publish_json(
        body={"ex_key": "ex_value"},
        url="https://httpstat.us/200",
        flow_control={
            "key": "flow-key",
            "parallelism": "3",
            "rate_per_second": "4"
        },
    )

    message = await async_client.message.get(result.message_id)

    assert message.flow_control_key == "flow-key"
    assert message.parallelism == 3
    assert message.rate_per_second == 4


@pytest.mark.asyncio
async def test_batch_with_flow_control_async(
    async_client: AsyncQStash,
) -> None:
    result = await async_client.message.batch_json(
        [
            BatchJsonRequest(
                body={"ex_key": "ex_value"},
                url="https://httpstat.us/200",
                flow_control=FlowControl(
                    key="flow-key-1",
                    rate_per_second="1",
                ),
            ),
            BatchJsonRequest(
                body={"ex_key": "ex_value"},
                url="https://httpstat.us/200",
                flow_control=FlowControl(
                    key="flow-key-2",
                    parallelism="5",
                ),
            ),
        ]
    )

    message1 = await async_client.message.get(result[0].message_id)
    message2 = await async_client.message.get(result[1].message_id)

    assert message1.flow_control_key == "flow-key-1"
    assert message1.parallelism is None
    assert message1.rate_per_second == 1

    assert message2.flow_control_key == "flow-key-2"
    assert message2.parallelism == 5
    assert message2.rate_per_second is None
