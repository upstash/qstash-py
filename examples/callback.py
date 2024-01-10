"""
Publish a message to a URL and send the response to a callback URL.

This is useful if you have a time consuming API call (ie: OpenAI)
and you want to send the response to your API URL without having
to wait for the response in a serverless function.
"""
from upstash_qstash import Client
from qstash_token import QSTASH_TOKEN


def main():
    client = Client(QSTASH_TOKEN)
    client.publish_json(
        {
            "url": "https://expensive-ai.requestcatcher.com",
            "callback": "https://py-qstash-testing.requestcatcher.com",
            "failure_callback": "https://py-qstash-testing-failed.requestcatcher.com",
            # We want to send a GET request to https://expensive-ai... and have the response
            # sent to https://py-qstash-testing...
            "method": "GET",
        }
    )


if __name__ == "__main__":
    main()
