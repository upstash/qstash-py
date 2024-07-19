"""
Create a chat completion request and send the response to a callback URL.

This is useful to send the response to your API without having
to wait for the response in a serverless function.
"""

from qstash import QStash
from qstash.chat import upstash


def main():
    client = QStash(
        token="<QSTASH-TOKEN>",
    )

    client.message.publish_json(
        api={"name": "llm", "provider": upstash()},
        body={
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [
                {
                    "role": "user",
                    "content": "What is the capital of Turkey?",
                }
            ],
        },
        callback="https://example-cb.com",
        # We want to send the response to https://example-cb.com
    )


if __name__ == "__main__":
    main()
