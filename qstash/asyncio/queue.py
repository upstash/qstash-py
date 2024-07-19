from typing import List

from qstash.asyncio.http import AsyncHttpClient
from qstash.queue import Queue, parse_queue_response, prepare_upsert_body


class AsyncQueueApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def upsert(
        self,
        queue: str,
        *,
        parallelism: int = 1,
        paused: bool = False,
    ) -> None:
        """
        Updates or creates a queue.

        :param queue: The name of the queue.
        :param parallelism: The number of parallel consumers consuming from the queue.
        :param paused: Whether to pause the queue or not. A paused queue will not
            deliver new messages until it is resumed.
        """
        body = prepare_upsert_body(queue, parallelism, paused)

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

    async def pause(self, queue: str) -> None:
        """
        Pauses the queue.

        A paused queue will not deliver messages until
        it is resumed.
        """
        await self._http.request(
            path=f"/v2/queues/{queue}/pause",
            method="POST",
            parse_response=False,
        )

    async def resume(self, queue: str) -> None:
        """
        Resumes the queue.
        """
        await self._http.request(
            path=f"/v2/queues/{queue}/resume",
            method="POST",
            parse_response=False,
        )
