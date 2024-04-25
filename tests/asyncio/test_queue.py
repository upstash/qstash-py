import pytest
from upstash_qstash.asyncio import Client
from qstash_tokens import QSTASH_TOKEN


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


@pytest.mark.asyncio
async def test_queue_upsert(client):
    queue_name = "test_queue"
    queue = client.queue({"queue_name": queue_name})

    print("Creating queue with parallelism 1")
    await queue.upsert({"parallelism": 1})

    print("Verifying queue details")
    queue_details = await queue.get()
    assert queue_details["name"] == queue_name
    assert queue_details["parallelism"] == 1

    print("Updating queue parallelism to 2")
    await queue.upsert({"parallelism": 2})

    print("Verifying queue details")
    queue_details = await queue.get()
    assert queue_details["parallelism"] == 2

    print("Making sure queue list returns the queue")
    all_queues = await queue.list()
    assert queue_name in map(lambda q: q["name"], all_queues)

    print("Deleting queue")
    await queue.delete()

    print("Making sure queue list does not return the queue")
    all_queues = await queue.list()
    assert queue_name not in map(lambda q: q["name"], all_queues)


@pytest.mark.asyncio
async def test_no_queue_name(client):
    queue = client.queue()

    print("Trying to upsert without a queue name")
    with pytest.raises(ValueError):
        await queue.upsert({"parallelism": 1})

    print("Trying to get without a queue name")
    with pytest.raises(ValueError):
        await queue.get()

    print("Trying to delete without a queue name")
    with pytest.raises(ValueError):
        await queue.delete()

    print("Trying to enqueue without a queue name")
    with pytest.raises(ValueError):
        await queue.enqueue({"url": "https://example.com"})

    with pytest.raises(ValueError):
        await queue.enqueue_json({"url": "https://example.com"})

    print("Should be able to list queues without a queue name")
    all_queues = await queue.list()
    print(all_queues)
    assert isinstance(all_queues, list)


@pytest.mark.asyncio
async def test_enqueue(client):
    queue_name = "test_queue"
    queue = client.queue({"queue_name": queue_name})

    print("Creating queue with parallelism 1")
    await queue.upsert({"parallelism": 1})

    print("Enqueueing message to the queue")
    res = await queue.enqueue_json(
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
    await queue.delete()
