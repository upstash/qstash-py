from typing import Callable

import pytest

from qstash import QStash
from qstash.chat import upstash, openai
from qstash.errors import QStashError
from qstash.event import EventState
from qstash.message import (
    BatchJsonRequest,
    BatchRequest,
    BatchResponse,
    EnqueueResponse,
    PublishResponse,
)
from tests import assert_eventually, OPENAI_API_KEY


def assert_delivered_eventually(client: QStash, msg_id: str) -> None:
    def assertion() -> None:
        events = client.event.list(
            filter={
                "message_id": msg_id,
                "state": EventState.DELIVERED,
            }
        ).events

        assert len(events) == 1

    assert_eventually(
        assertion,
        initial_delay=1.0,
        retry_delay=1.0,
        timeout=60.0,
    )


def test_publish_to_url(client: QStash) -> None:
    res = client.message.publish(
        body="test-body",
        url="https://httpstat.us/200",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    assert_delivered_eventually(client, res.message_id)


def test_publish_to_url_json(client: QStash) -> None:
    res = client.message.publish_json(
        body={"ex_key": "ex_value"},
        url="https://httpstat.us/200",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    assert_delivered_eventually(client, res.message_id)


def test_disallow_multiple_destinations(client: QStash) -> None:
    with pytest.raises(QStashError):
        client.message.publish_json(
            url="https://httpstat.us/200",
            url_group="test-url-group",
        )

    with pytest.raises(QStashError):
        client.message.publish_json(
            url="https://httpstat.us/200",
            api={"name": "llm", "provider": upstash()},
        )

    with pytest.raises(QStashError):
        client.message.publish_json(
            url_group="test-url-group",
            api={"name": "llm", "provider": upstash()},
        )


def test_batch(client: QStash) -> None:
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

    res = client.message.batch(messages)

    assert len(res) == N

    for r in res:
        assert isinstance(r, BatchResponse)
        assert len(r.message_id) > 0


def test_batch_json(client: QStash) -> None:
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

    res = client.message.batch_json(messages)

    assert len(res) == N

    for r in res:
        assert isinstance(r, BatchResponse)
        assert len(r.message_id) > 0


def test_publish_to_api_llm(client: QStash) -> None:
    res = client.message.publish_json(
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

    assert_delivered_eventually(client, res.message_id)


def test_batch_api_llm(client: QStash) -> None:
    res = client.message.batch_json(
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
                },  # type:ignore[arg-type]
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

    assert_delivered_eventually(client, res[0].message_id)
    assert_delivered_eventually(client, res[1].message_id)


def test_enqueue(
    client: QStash,
    cleanup_queue: Callable[[QStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(client, name)

    res = client.message.enqueue(
        queue=name,
        body="test-body",
        url="https://httpstat.us/200",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0


def test_enqueue_json(
    client: QStash,
    cleanup_queue: Callable[[QStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(client, name)

    res = client.message.enqueue_json(
        queue=name,
        body={"test": "body"},
        url="https://httpstat.us/200",
        headers={
            "test-header": "test-value",
        },
    )

    assert isinstance(res, EnqueueResponse)

    assert len(res.message_id) > 0


def test_enqueue_api_llm(
    client: QStash,
    cleanup_queue: Callable[[QStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(client, name)

    res = client.message.enqueue_json(
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


def test_publish_to_url_group(client: QStash) -> None:
    name = "python_url_group"
    client.url_group.delete(name)

    client.url_group.upsert_endpoints(
        url_group=name,
        endpoints=[
            {"url": "https://httpstat.us/200"},
            {"url": "https://httpstat.us/201"},
        ],
    )

    res = client.message.publish(
        body="test-body",
        url_group=name,
    )

    assert isinstance(res, list)
    assert len(res) == 2

    assert_delivered_eventually(client, res[0].message_id)
    assert_delivered_eventually(client, res[1].message_id)


def test_timeout(client: QStash) -> None:
    res = client.message.publish_json(
        body={"ex_key": "ex_value"},
        url="https://httpstat.us/200",
        timeout=90,
    )

    assert isinstance(res, PublishResponse)
    assert len(res.message_id) > 0

    assert_delivered_eventually(client, res.message_id)


def test_cancel_many(client: QStash) -> None:
    res0 = client.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res0, PublishResponse)

    res1 = client.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res1, PublishResponse)

    cancelled = client.message.cancel_many([res0.message_id, res1.message_id])

    assert cancelled == 2


def test_cancel_all(client: QStash) -> None:
    res0 = client.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res0, PublishResponse)

    res1 = client.message.publish(
        url="http://httpstat.us/404",
        retries=3,
    )

    assert isinstance(res1, PublishResponse)

    cancelled = client.message.cancel_all()

    assert cancelled >= 2


def test_publish_to_api_llm_custom_provider(client: QStash) -> None:
    res = client.message.publish_json(
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

    assert_delivered_eventually(client, res.message_id)


def test_enqueue_api_llm_custom_provider(
    client: QStash,
    cleanup_queue: Callable[[QStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(client, name)

    res = client.message.enqueue_json(
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
