"""
Publishes a JSON message with a 3s delay to a URL using QStash.
"""

from qstash import QStash


def main() -> None:
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

    print(res.message_id)  # type:ignore[union-attr]


if __name__ == "__main__":
    main()
