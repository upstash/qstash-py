from qstash import QStash
from qstash.message import PublishResponse
from tests import assert_eventually


def assert_failed_eventually(client: QStash, *msg_ids: str) -> None:
    def assertion() -> None:
        messages = client.dlq.list().messages

        matched_messages = [msg for msg in messages if msg.message_id in msg_ids]
        assert len(matched_messages) == len(msg_ids)

        for msg in matched_messages:
            dlq_msg = client.dlq.get(msg.dlq_id)
            assert dlq_msg.response_body == "404 Not Found"
            assert msg.response_body == "404 Not Found"
            assert dlq_msg.retry_delay_expression == "7000 * retried"
            assert msg.retry_delay_expression == "7000 * retried"

        if len(msg_ids) == 1:
            client.dlq.delete(matched_messages[0].dlq_id)
        else:
            deleted = client.dlq.delete_many([m.dlq_id for m in matched_messages])
            assert deleted == len(msg_ids)

        messages = client.dlq.list().messages
        matched = any(True for msg in messages if msg.message_id in msg_ids)
        assert not matched

    assert_eventually(
        assertion,
        initial_delay=2.0,
        retry_delay=1.0,
        timeout=10.0,
    )


def test_dlq_get_and_delete(client: QStash) -> None:
    res = client.message.publish_json(
        url="http://httpstat.us/404",
        retries=0,
        retry_delay="7000 * retried",
    )

    assert isinstance(res, PublishResponse)

    assert_failed_eventually(client, res.message_id)


def test_dlq_get_and_delete_many(client: QStash) -> None:
    msg_ids = []
    for _ in range(5):
        res = client.message.publish_json(
            url="http://httpstat.us/404",
            retries=0,
            retry_delay="7000 * retried",
        )

        assert isinstance(res, PublishResponse)
        msg_ids.append(res.message_id)

    assert_failed_eventually(client, *msg_ids)


def test_dlq_filter(client: QStash) -> None:
    res = client.message.publish_json(
        url="http://httpstat.us/404",
        retries=0,
        retry_delay="7000 * retried",
    )

    assert isinstance(res, PublishResponse)

    def assertion() -> None:
        messages = client.dlq.list(
            filter={"message_id": res.message_id},
            count=1,
        ).messages

        assert len(messages) == 1
        assert messages[0].message_id == res.message_id
        assert messages[0].retry_delay_expression == "7000 * retried"

        client.dlq.delete(messages[0].dlq_id)

    assert_eventually(
        assertion,
        initial_delay=2.0,
        retry_delay=1.0,
        timeout=10.0,
    )
