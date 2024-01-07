"""
Uses asyncio to asynchronously publish a JSON message with a 3s delay to a URL using QStash.
"""

import dotenv
import asyncio
from upstash_qstash.asyncio import Client

QSTASH_TOKEN = dotenv.dotenv_values()["QSTASH_TOKEN"]


async def main():
    client = Client(QSTASH_TOKEN)
    res = await client.publish_json(
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


if __name__ == "__main__":
    asyncio.run(main())
