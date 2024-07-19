from typing import Callable

from qstash import QStash


def test_queue(
    client: QStash,
    cleanup_queue: Callable[[QStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(client, name)

    client.queue.upsert(queue=name, parallelism=1)

    queue = client.queue.get(name)
    assert queue.name == name
    assert queue.parallelism == 1

    client.queue.upsert(queue=name, parallelism=2)

    queue = client.queue.get(name)
    assert queue.name == name
    assert queue.parallelism == 2

    all_queues = client.queue.list()
    assert any(True for q in all_queues if q.name == name)

    client.queue.delete(name)

    all_queues = client.queue.list()
    assert not any(True for q in all_queues if q.name == name)


def test_queue_pause_resume(
    client: QStash,
    cleanup_queue: Callable[[QStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(client, name)

    client.queue.upsert(queue=name)

    queue = client.queue.get(name)
    assert queue.paused is False

    client.queue.pause(name)

    queue = client.queue.get(name)
    assert queue.paused is True

    client.queue.resume(name)

    queue = client.queue.get(name)
    assert queue.paused is False

    client.queue.upsert(name, paused=True)

    queue = client.queue.get(name)
    assert queue.paused is True

    client.queue.upsert(name, paused=False)

    queue = client.queue.get(name)
    assert queue.paused is False
