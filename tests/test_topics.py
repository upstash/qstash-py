import pytest
import time
from upstash_qstash import Client
from qstash_token import QSTASH_TOKEN


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


def test_topic_lifecycle(client):
    """
    Test creating, listing, getting, publishing to, removing endpoints from, and deleting a topic
    """
    topic_name = "sdk_test_topic_" + str(time.time())
    topics = client.topics()

    print(f"Creating topic {topic_name} and adding endpoints")
    topics.add_endpoints(
        {
            "name": topic_name,
            "endpoints": [
                {"url": "https://qstash-endpoint1.requestcatcher.com"},
                {"url": "https://qstash-endpoint2.requestcatcher.com"},
            ],
        }
    )

    print("Waiting 1 second for topic update")
    time.sleep(1)

    print(f"Getting details of topic {topic_name}")
    get_res = topics.get(topic_name)
    assert get_res["name"] == topic_name
    assert any(
        endpoint["url"] == "https://qstash-endpoint1.requestcatcher.com"
        for endpoint in get_res["endpoints"]
    )
    assert any(
        endpoint["url"] == "https://qstash-endpoint2.requestcatcher.com"
        for endpoint in get_res["endpoints"]
    )

    print(f"Checking if '{topic_name}' is in the list of all topics")
    list_res = topics.list()
    assert any(topic["name"] == topic_name for topic in list_res)

    print(f"Publishing to {topic_name}")
    publish_res = client.publish_json(
        {
            "topic": topic_name,
        }
    )

    assert publish_res[0]["messageId"] is not None

    print(f"Removing endpoint1 from '{topic_name}'")
    topics.remove_endpoints(
        {
            "name": topic_name,
            "endpoints": [
                {"url": "https://qstash-endpoint1.requestcatcher.com"},
            ],
        }
    )

    print("Waiting 1 second for topic update")
    time.sleep(1)

    print(f"Checking if endpoint1 have been removed from '${topic_name}'")
    get_res = topics.get(topic_name)
    assert not any(
        endpoint["url"] == "https://qstash-endpoint1.requestcatcher.com"
        for endpoint in get_res["endpoints"]
    )

    print(f"Deleting '${topic_name}'")
    topics.delete(topic_name)

    print("Waiting 1 second for topic to be deleted")
    time.sleep(1)

    print(f"Checking if '${topic_name}' has been deleted")
    list_res = topics.list()
    assert not any(topic["name"] == topic_name for topic in list_res)
