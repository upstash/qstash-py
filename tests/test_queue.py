from typing import Callable

from upstash_qstash import QStash


def test_queue(
    qstash: QStash,
    cleanup_queue: Callable[[QStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(qstash, name)

    qstash.queue.upsert(queue=name, parallelism=1)

    queue = qstash.queue.get(name)
    assert queue.name == name
    assert queue.parallelism == 1

    qstash.queue.upsert(queue=name, parallelism=2)

    queue = qstash.queue.get(name)
    assert queue.name == name
    assert queue.parallelism == 2

    all_queues = qstash.queue.list()
    assert any(True for q in all_queues if q.name == name)

    qstash.queue.delete(name)

    all_queues = qstash.queue.list()
    assert not any(True for q in all_queues if q.name == name)


def test_queue_pause_resume(
    qstash: QStash,
    cleanup_queue: Callable[[QStash, str], None],
) -> None:
    name = "test_queue"
    cleanup_queue(qstash, name)

    qstash.queue.upsert(queue=name)

    queue = qstash.queue.get(name)
    assert queue.paused is False

    qstash.queue.pause(name)

    queue = qstash.queue.get(name)
    assert queue.paused is True

    qstash.queue.resume(name)

    queue = qstash.queue.get(name)
    assert queue.paused is False

    qstash.queue.upsert(name, paused=True)

    queue = qstash.queue.get(name)
    assert queue.paused is True

    qstash.queue.upsert(name, paused=False)

    queue = qstash.queue.get(name)
    assert queue.paused is False
