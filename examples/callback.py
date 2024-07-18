"""
Publish a message to a URL and send the response to a callback URL.

This is useful if you have a time consuming API call
and you want to send the response to your API URL without having
to wait for the response in a serverless function.
"""

from qstash import QStash


def main():
    client = QStash(
        token="<QSTASH-TOKEN>",
    )

    client.message.publish_json(
        url="https://expensive.com",
        callback="https://example-cb.com",
        # We want to send a GET request to https://expensive.com and have the response
        # sent to https://example-cb.com
        method="GET",
    )


if __name__ == "__main__":
    main()
