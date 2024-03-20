import pytest
import time
from upstash_qstash import Client
from qstash_tokens import QSTASH_TOKEN


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


def test_dlq(client):
    print("Publishing to a failed endpoint")
    pub_res = client.publish_json({"url": "http://httpstat.us/404", "retries": 0})

    msg_id = pub_res["messageId"]
    assert msg_id is not None

    print("Waiting 5 seconds for event to be delivered")
    time.sleep(5)

    print("Checking if message is in DLQ")
    dlq = client.dlq()
    all_messages = dlq.list_messages()["messages"]

    msg_sent = list(filter(lambda msg: msg["messageId"] == msg_id, all_messages))
    # If this is failing, it's likely because the message hasn't been delivered yet.
    # Try increasing the sleep time.
    assert len(msg_sent) == 1

    dlq_id = msg_sent[0]["dlqId"]

    print("Getting message from DLQ")
    msg = dlq.get(dlq_id)
    assert msg["responseBody"] == "404 Not Found"

    print("Deleting message from DLQ")
    dlq.delete(dlq_id)

    print("Checking if message is deleted from DLQ")
    all_messages_after_delete = dlq.list_messages()["messages"]
    msg_deleted = list(
        filter(lambda msg: msg["messageId"] == msg_id, all_messages_after_delete)
    )
    assert len(msg_deleted) == 0


def test_dlq_delete_many(client):
    print("Publishing 5 messages to a failed endpoint")
    msg_ids = []
    for _ in range(5):
        pub_res = client.publish_json({"url": "http://httpstat.us/404", "retries": 0})
        msg_ids.append(pub_res["messageId"])
    assert len(msg_ids) == 5

    print("Waiting 5 seconds for events to be delivered")
    time.sleep(5)

    print("Checking if messages are in DLQ")
    dlq = client.dlq()
    all_messages = dlq.list_messages()["messages"]
    msg_sent = list(filter(lambda msg: msg["messageId"] in msg_ids, all_messages))
    assert len(msg_sent) == 5

    print("Deleting messages from DLQ")
    dlq_ids = [msg["dlqId"] for msg in msg_sent]
    dlq.deleteMany({"dlq_ids": dlq_ids})

    print("Checking if messages are deleted from DLQ")
    all_messages_after_delete = dlq.list_messages()["messages"]
    msg_deleted = list(
        filter(lambda msg: msg["messageId"] in msg_ids, all_messages_after_delete)
    )
    assert len(msg_deleted) == 0
    assert len(all_messages_after_delete) == len(all_messages) - 5
