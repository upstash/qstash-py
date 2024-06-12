from typing import List

from upstash_qstash.asyncio.http import AsyncHttpClient
from upstash_qstash.queue import Queue, parse_queue_response, prepare_upsert_body


class AsyncQueueApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def upsert(self, queue: str, *, parallelism: int = 1) -> None:
        """
        Updates or creates a queue.

        :param queue: The name of the queue.
        :param parallelism: The number of parallel consumers consuming from the queue.
        """
        body = prepare_upsert_body(queue, parallelism)

        await self._http.request(
            path="/v2/queues",
            method="POST",
            headers={"Content-Type": "application/json"},
            body=body,
            parse_response=False,
        )

    async def get(self, queue: str) -> Queue:
        """
        Gets the queue by its name.
        """
        response = await self._http.request(
            path=f"/v2/queues/{queue}",
            method="GET",
        )

        return parse_queue_response(response)

    async def list(self) -> List[Queue]:
        """
        Lists all the queues.
        """
        response = await self._http.request(
            path="/v2/queues",
            method="GET",
        )

        return [parse_queue_response(r) for r in response]

    async def delete(self, queue: str) -> None:
        """
        Deletes the queue.
        """
        await self._http.request(
            path=f"/v2/queues/{queue}",
            method="DELETE",
            parse_response=False,
        )
