from typing import Callable

import pytest

from qstash import AsyncQStash
from qstash.chat import openai
from qstash.errors import QStashError
from qstash.log import LogState
from qstash.message import (
    BatchJsonRequest,
    BatchRequest,
    BatchResponse,
    EnqueueResponse,
    PublishResponse,
    FlowControl,
)
from tests import assert_eventually_async, OPENAI_API_KEY


async def assert_delivered_eventually_async(
    async_client: AsyncQStash, msg_id: str
) -> None:
    async def assertion() -> None:
        logs = (
            await async_client.log.list(
                filter={
                    "message_id": msg_id,
                    "state": LogState.DELIVERED,
                }
            )
        ).logs

        assert len(logs) == 1

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
        method="GET",
        url="https://mock.httpstatus.io/200",
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
        method="GET",
        url="https://mock.httpstatus.io/200",
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
            method="GET",
            url="https://mock.httpstatus.io/200",
            url_group="test-url-group",
        )

    with pytest.raises(QStashError):
        await async_client.message.publish_json(
            method="GET",
            url="https://mock.httpstatus.io/200",
            api={"name": "llm", "provider": openai(OPENAI_API_KEY)},
        )

    with pytest.raises(QStashError):
        await async_client.message.publish_json(
            url_group="test-url-group",
            api={"name": "llm", "provider": openai(OPENAI_API_KEY)},
        )


@pytest.mark.asyncio
async def test_batch_async(async_client: AsyncQStash) -> None:
    N = 3
    messages = []
    for i in range(N):
        messages.append(
            BatchRequest(
                body=f"hi {i}",
                method="GET",
                url="https://mock.httpstatus.io/200",
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
                method="GET",
                url="https://mock.httpstatus.io/200",
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
        api={"name": "llm", "provider": openai(OPENAI_API_KEY)},
        body={
            "model": "gpt-3.5-turbo",
            "messages": [
                {
                    "role": "user",
                    "content": "just say hello",
                }
            ],
        },
        callback="https://mock.httpstatus.io/200",
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    await assert_delivered_eventually_async(async_client, res.message_id)


@pytest.mark.asyncio
async def test_batch_api_llm_async(async_client: AsyncQStash) -> None:
    res = await async_client.message.batch_json(
        [
            {
                "api": {"name": "llm", "provider": openai(OPENAI_API_KEY)},
                "body": {
                    "model": "gpt-3.5-turbo",
                    "messages": [
                        {
                            "role": "user",
                            "content": "just say hello",
                        }
                    ],
                },
                "callback": "https://mock.httpstatus.io/200",
            },
        ]
    )

    assert len(res) == 1

    assert isinstance(res[0], BatchResponse)
    assert len(res[0].message_id) > 0

    await assert_delivered_eventually_async(async_client, res[0].message_id)


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
        method="GET",
        url="https://mock.httpstatus.io/200",
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
        method="GET",
        url="https://mock.httpstatus.io/200",
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
            "model": "gpt-3.5-turbo",
            "messages": [
                {
                    "role": "user",
                    "content": "just say hello",
                }
            ],
        },
        api={"name": "llm", "provider": openai(OPENAI_API_KEY)},
        callback="https://mock.httpstatus.io/200",
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
            {"url": "https://mock.httpstatus.io/200"},
            {"url": "https://mock.httpstatus.io/201"},
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
        method="GET",
        url="https://mock.httpstatus.io/200",
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
        api={"name": "llm", "provider": openai(OPENAI_API_KEY)},
        body={
            "model": "gpt-3.5-turbo",
            "messages": [
                {
                    "role": "user",
                    "content": "just say hello",
                }
            ],
        },
        callback="https://mock.httpstatus.io/200",
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
        api={"name": "llm", "provider": openai(OPENAI_API_KEY)},
        callback="https://mock.httpstatus.io/200",
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0


@pytest.mark.asyncio
async def test_publish_with_flow_control_async(
    async_client: AsyncQStash,
) -> None:
    result = await async_client.message.publish_json(
        body={"ex_key": "ex_value"},
        url="https://mock.httpstatus.io/200?sleep=30000",
        flow_control=FlowControl(key="flow-key", parallelism=3, rate=4, period=2),
    )

    assert isinstance(result, PublishResponse)
    message = await async_client.message.get(result.message_id)

    flow_control = message.flow_control
    assert flow_control is not None
    assert flow_control.key == "flow-key"
    assert flow_control.parallelism == 3
    assert flow_control.rate == 4
    assert flow_control.period == 2


@pytest.mark.asyncio
async def test_batch_with_flow_control_async(
    async_client: AsyncQStash,
) -> None:
    result = await async_client.message.batch_json(
        [
            BatchJsonRequest(
                body={"ex_key": "ex_value"},
                url="https://mock.httpstatus.io/200?sleep=30000",
                flow_control=FlowControl(key="flow-key-1", rate=1),
            ),
            BatchJsonRequest(
                body={"ex_key": "ex_value"},
                url="https://mock.httpstatus.io/200?sleep=30000",
                flow_control=FlowControl(key="flow-key-2", rate=23, period="1h30m3s"),
            ),
            BatchJsonRequest(
                body={"ex_key": "ex_value"},
                url="https://mock.httpstatus.io/200?sleep=30000",
                flow_control=FlowControl(key="flow-key-3", parallelism=5),
            ),
        ]
    )

    assert isinstance(result[0], BatchResponse)
    message1 = await async_client.message.get(result[0].message_id)

    flow_control1 = message1.flow_control
    assert flow_control1 is not None
    assert flow_control1.key == "flow-key-1"
    assert flow_control1.parallelism is None
    assert flow_control1.rate == 1
    assert flow_control1.period == 1

    assert isinstance(result[1], BatchResponse)
    message2 = await async_client.message.get(result[1].message_id)

    flow_control2 = message2.flow_control
    assert flow_control2 is not None
    assert flow_control2.key == "flow-key-2"
    assert flow_control2.parallelism is None
    assert flow_control2.rate == 23
    assert flow_control2.period == 5403

    assert isinstance(result[2], BatchResponse)
    message3 = await async_client.message.get(result[2].message_id)

    flow_control3 = message3.flow_control
    assert flow_control3 is not None
    assert flow_control3.key == "flow-key-3"
    assert flow_control3.parallelism == 5
    assert flow_control3.rate is None
    assert flow_control3.period == 1
