import pytest

from qstash_tokens import QSTASH_TOKEN
from upstash_qstash import Client


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


def test_queue_upsert(client):
    queue_name = "test_queue"
    queue = client.queue({"queue_name": queue_name})

    print("Creating queue with parallelism 1")
    queue.upsert({"parallelism": 1})

    print("Verifying queue details")
    queue_details = queue.get()
    assert queue_details["name"] == queue_name
    assert queue_details["parallelism"] == 1

    print("Updating queue parallelism to 2")
    queue.upsert({"parallelism": 2})

    print("Verifying queue details")
    queue_details = queue.get()
    assert queue_details["parallelism"] == 2

    print("Making sure queue list returns the queue")
    all_queues = queue.list()
    assert queue_name in map(lambda q: q["name"], all_queues)

    print("Deleting queue")
    queue.delete()

    print("Making sure queue list does not return the queue")
    all_queues = queue.list()
    assert queue_name not in map(lambda q: q["name"], all_queues)


def test_no_queue_name(client):
    queue = client.queue()

    print("Trying to upsert without a queue name")
    with pytest.raises(ValueError):
        queue.upsert({"parallelism": 1})

    print("Trying to get without a queue name")
    with pytest.raises(ValueError):
        queue.get()

    print("Trying to delete without a queue name")
    with pytest.raises(ValueError):
        queue.delete()

    print("Trying to enqueue without a queue name")
    with pytest.raises(ValueError):
        queue.enqueue({"url": "https://example.com"})

    with pytest.raises(ValueError):
        queue.enqueue_json({"url": "https://example.com"})

    print("Should be able to list queues without a queue name")
    all_queues = queue.list()
    print(all_queues)
    assert isinstance(all_queues, list)


def test_enqueue(client):
    queue_name = "test_queue"
    queue = client.queue({"queue_name": queue_name})

    print("Creating queue with parallelism 1")
    queue.upsert({"parallelism": 1})

    print("Enqueueing message to the queue")
    res = queue.enqueue_json(
        {
            "body": {"ex_key": "ex_value"},
            "url": "https://meshan456.requestcatcher.com/test",
            "headers": {
                "test-header": "test-value",
            },
        }
    )

    print("Verifying enqueue response")
    assert res["messageId"] is not None

    print("Deleting queue")
    queue.delete()


def test_enqueue_api_llm(client):
    # not a proper test, because of a dummy callback.
    queue = client.queue({"queue_name": "test_queue"})

    try:
        res = queue.enqueue_json(
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
                "callback": "https://example.com/",
            }
        )

        assert res["messageId"] is not None
    finally:
        queue.delete()
