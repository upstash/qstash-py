"""
Publishes a JSON message with a 3s delay to a URL using QStash.
"""

from upstash_qstash import Client
from qstash_tokens import QSTASH_TOKEN


def main():
    client = Client(QSTASH_TOKEN)
    res = client.publish_json(
        {
            "url": "https://py-qstash-testing.requestcatcher.com",
            "body": {"hello": "world"},
            "headers": {
                "test-header": "test-value",
            },
            "delay": 3,
        }
    )

    print(res["messageId"])

    """
    Received from QStash:
    POST / HTTP/1.1

    Host: py-qstash-testing.requestcatcher.com
    Accept-Encoding: gzip
    Content-Length: 18
    Content-Type: application/json
    Test-Header: test-value
    Upstash-Message-Id: msg_...
    Upstash-Retried: 0
    Upstash-Signature: ey...
    User-Agent: Upstash-QStash

    {"hello": "world"}
    """


if __name__ == "__main__":
    main()
