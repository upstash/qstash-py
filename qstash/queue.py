import dataclasses
import json
from typing import Any, Dict, List

from qstash.http import HttpClient


@dataclasses.dataclass
class Queue:
    name: str
    """The name of the queue."""

    parallelism: int
    """The number of parallel consumers consuming from the queue."""

    created_at: int
    """The creation time of the queue, in unix milliseconds."""

    updated_at: int
    """The last update time of the queue, in unix milliseconds."""

    lag: int
    """The number of unprocessed messages that exist in the queue."""

    paused: bool
    """Whether the queue is paused or not."""


def prepare_upsert_body(queue: str, parallelism: int, paused: bool) -> str:
    return json.dumps(
        {
            "queueName": queue,
            "parallelism": parallelism,
            "paused": paused,
        }
    )


def parse_queue_response(response: Dict[str, Any]) -> Queue:
    return Queue(
        name=response["name"],
        parallelism=response["parallelism"],
        created_at=response["createdAt"],
        updated_at=response["updatedAt"],
        lag=response["lag"],
        paused=response["paused"],
    )


class QueueApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def upsert(
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

        self._http.request(
            path="/v2/queues",
            method="POST",
            headers={"Content-Type": "application/json"},
            body=body,
            parse_response=False,
        )

    def get(self, queue: str) -> Queue:
        """
        Gets the queue by its name.
        """
        response = self._http.request(
            path=f"/v2/queues/{queue}",
            method="GET",
        )

        return parse_queue_response(response)

    def list(self) -> List[Queue]:
        """
        Lists all the queues.
        """
        response = self._http.request(
            path="/v2/queues",
            method="GET",
        )

        return [parse_queue_response(r) for r in response]

    def delete(self, queue: str) -> None:
        """
        Deletes the queue.
        """
        self._http.request(
            path=f"/v2/queues/{queue}",
            method="DELETE",
            parse_response=False,
        )

    def pause(self, queue: str) -> None:
        """
        Pauses the queue.

        A paused queue will not deliver messages until
        it is resumed.
        """
        self._http.request(
            path=f"/v2/queues/{queue}/pause",
            method="POST",
            parse_response=False,
        )

    def resume(self, queue: str) -> None:
        """
        Resumes the queue.
        """
        self._http.request(
            path=f"/v2/queues/{queue}/resume",
            method="POST",
            parse_response=False,
        )
