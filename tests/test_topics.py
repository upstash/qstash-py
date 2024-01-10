import pytest
import time
from upstash_qstash import Client
from qstash_token import QSTASH_TOKEN


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


def test_topic_lifecycle(client):
    topics = client.topics()

    print("Creating topic 'sdk_test_topic' and adding endpoints")
    topics.add_endpoints(
        {
            "name": "sdk_test_topic",
            "endpoints": [
                {"url": "https://example.com/endpoint1"},
                {"url": "https://example.com/endpoint2"},
            ],
        }
    )

    print("Waiting 1 second for topic update")
    time.sleep(1)

    print("Getting details of 'sdk_test_topic'")
    get_res = topics.get("sdk_test_topic")
    assert get_res["name"] == "sdk_test_topic"
    assert any(
        endpoint["url"] == "https://example.com/endpoint1"
        for endpoint in get_res["endpoints"]
    )
    assert any(
        endpoint["url"] == "https://example.com/endpoint2"
        for endpoint in get_res["endpoints"]
    )

    print("Checking if 'sdk_test_topic' is in the list of all topics")
    list_res = topics.list()
    assert any(topic["name"] == "sdk_test_topic" for topic in list_res)

    print("Removing endpoint1 from 'sdk_test_topic'")
    topics.remove_endpoints(
        {
            "name": "sdk_test_topic",
            "endpoints": [
                {"url": "https://example.com/endpoint1"},
            ],
        }
    )

    print("Waiting 1 second for topic update")
    time.sleep(1)

    print("Checking if endpoint1 have been removed from 'sdk_test_topic'")
    get_res = topics.get("sdk_test_topic")
    assert not any(
        endpoint["url"] == "https://example.com/endpoint1"
        for endpoint in get_res["endpoints"]
    )

    print("Deleting 'sdk_test_topic'")
    topics.delete("sdk_test_topic")

    print("Waiting 1 second for topic to be deleted")
    time.sleep(1)

    print("Checking if 'sdk_test_topic' has been deleted")
    list_res = topics.list()
    assert not any(topic["name"] == "sdk_test_topic" for topic in list_res)
