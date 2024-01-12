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

    print("Deleting message from DLQ")
    dlq.delete(dlq_id)

    print("Checking if message is deleted from DLQ")
    all_messages_after_delete = dlq.list_messages()["messages"]
    msg_deleted = list(
        filter(lambda msg: msg["messageId"] == msg_id, all_messages_after_delete)
    )
    assert len(msg_deleted) == 0
