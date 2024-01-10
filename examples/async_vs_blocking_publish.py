"""
Send 3 messages to a URL using the blocking client and 3 messages to a URL using the async client.
Demonstrate the difference in behavior between blocking and async operations with added artificial delay.
"""

from upstash_qstash import Client as BlockingClient
from upstash_qstash.asyncio import Client as AsyncClient
import asyncio
import time
from qstash_token import QSTASH_TOKEN

# Artificial delay (in seconds)
DELAY = 2


async def main():
    blocking_client = BlockingClient(QSTASH_TOKEN)
    async_client = AsyncClient(QSTASH_TOKEN)

    # Async client
    async_tasks = []
    start_time = time.time()
    for i in range(3):
        task = asyncio.create_task(async_publish_with_delay(async_client, i, DELAY))
        async_tasks.append(task)

    await asyncio.gather(*async_tasks)
    print(f"Async: All messages published by {time.time() - start_time:.2f}s")

    # Blocking client
    start_time = time.time()
    for i in range(3):
        blocking_client.publish_json(
            {
                "url": "https://py-qstash-testing.requestcatcher.com",
                "body": {"Blocking iteration": i},
            }
        )
        time.sleep(DELAY)
        print(f"Blocking: Published message {i} at {time.time() - start_time:.2f}s")


async def async_publish_with_delay(client, iteration, delay):
    """Helper function to publish a message and then wait asynchronously."""
    await client.publish_json(
        {
            "url": "https://py-qstash-testing.requestcatcher.com",
            "body": {"Async iteration": iteration},
        }
    )
    await asyncio.sleep(delay)
    print(f"Async: Published message {iteration} after waiting {delay}s")


if __name__ == "__main__":
    asyncio.run(main())
