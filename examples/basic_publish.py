"""
Publishes a JSON message with a 3s delay to a URL using QStash.
"""

from qstash import QStash


def main():
    client = QStash(
        token="<QSTASH-TOKEN>",
    )

    res = client.message.publish_json(
        url="https://example.com",
        body={"hello": "world"},
        headers={
            "test-header": "test-value",
        },
        delay="3s",
    )

    print(res.message_id)


if __name__ == "__main__":
    main()
