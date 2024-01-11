import pytest
import asyncio
from upstash_qstash.asyncio import Client
<<<<<<< HEAD
from qstash_tokens import QSTASH_TOKEN
=======
from qstash_token import QSTASH_TOKEN
>>>>>>> 0e2c55f1d649fc156ebe14bc7edde5f69e40fd22


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


@pytest.mark.asyncio
async def test_dlq(client):
    print("Publishing to a failed endpoint")
    pub_res = await client.publish_json({"url": "http://httpstat.us/404", "retries": 0})

    msg_id = pub_res["messageId"]
    assert msg_id is not None

    print("Waiting 3 seconds for event to be delivered")
    await asyncio.sleep(3)

    print("Checking if message is in DLQ")
    dlq = client.dlq()
    all_messages_res = await dlq.list_messages()
    all_messages = all_messages_res["messages"]

    msg_sent = list(filter(lambda msg: msg["messageId"] == msg_id, all_messages))
    # If this is failing, it's likely because the message hasn't been delivered yet.
    # Try increasing the sleep time.
    assert len(msg_sent) == 1

    dlq_id = msg_sent[0]["dlqId"]

    print("Deleting message from DLQ")
    await dlq.delete(dlq_id)

    print("Checking if message is deleted from DLQ")
    all_messages_after_delete_res = await dlq.list_messages()
    all_messages_after_delete = all_messages_after_delete_res["messages"]
    msg_deleted = list(
        filter(lambda msg: msg["messageId"] == msg_id, all_messages_after_delete)
    )
    assert len(msg_deleted) == 0
