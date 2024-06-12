from tests import assert_eventually
from upstash_qstash import QStash
from upstash_qstash.message import PublishResponse


def assert_failed_eventually(qstash: QStash, *msg_ids: str) -> None:
    def assertion() -> None:
        messages = qstash.dlq.list().messages

        matched_messages = [msg for msg in messages if msg.message_id in msg_ids]
        assert len(matched_messages) == len(msg_ids)

        for msg in matched_messages:
            dlq_msg = qstash.dlq.get(msg.dlq_id)
            assert dlq_msg.response_body == "404 Not Found"
            assert msg.response_body == "404 Not Found"

        if len(msg_ids) == 1:
            qstash.dlq.delete(matched_messages[0].dlq_id)
        else:
            deleted = qstash.dlq.delete_many([m.dlq_id for m in matched_messages])
            assert deleted == len(msg_ids)

        messages = qstash.dlq.list().messages
        matched = any(True for msg in messages if msg.message_id in msg_ids)
        assert not matched

    assert_eventually(
        assertion,
        initial_delay=2.0,
        retry_delay=1.0,
        timeout=10.0,
    )


def test_dlq_get_and_delete(qstash: QStash) -> None:
    res = qstash.message.publish_json(
        url="http://httpstat.us/404",
        retries=0,
    )

    assert isinstance(res, PublishResponse)

    assert_failed_eventually(qstash, res.message_id)


def test_dlq_get_and_delete_many(qstash: QStash) -> None:
    msg_ids = []
    for _ in range(5):
        res = qstash.message.publish_json(
            url="http://httpstat.us/404",
            retries=0,
        )

        assert isinstance(res, PublishResponse)
        msg_ids.append(res.message_id)

    assert_failed_eventually(qstash, *msg_ids)
