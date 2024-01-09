import pytest
import dotenv
import time
from upstash_qstash import Client

QSTASH_TOKEN = dotenv.dotenv_values()["QSTASH_TOKEN"]


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


def test_get_message(client):
    res = client.publish_json(
        {
            "body": {"ex_key": "ex_value"},
            "url": "https://example.com",
            "headers": {
                "test-header": "test-value",
            },
        }
    )

    print("Waiting 1 seconds for event to be delivered")
    time.sleep(1)

    messages = client.messages()
    msg = messages.get(res["messageId"])
    assert msg["messageId"] == res["messageId"]
    assert msg["url"] == "https://example.com"
    assert msg["body"] == '{"ex_key": "ex_value"}'


def test_delete_message(client):
    pass
