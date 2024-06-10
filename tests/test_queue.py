from upstash_qstash import QStash


def test_queue(qstash: QStash) -> None:
    qstash.queue.upsert(queue="test_queue", parallelism=1)

    queue = qstash.queue.get("test_queue")
    assert queue.name == "test_queue"
    assert queue.parallelism == 1

    qstash.queue.upsert(queue="test_queue", parallelism=2)

    queue = qstash.queue.get("test_queue")
    assert queue.name == "test_queue"
    assert queue.parallelism == 2

    all_queues = qstash.queue.list()
    assert any(True for q in all_queues if q.name == "test_queue")

    qstash.queue.delete("test_queue")

    all_queues = qstash.queue.list()
    assert not any(True for q in all_queues if q.name == "test_queue")
