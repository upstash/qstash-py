import dataclasses
import json
from typing import Any, Dict, List

from upstash_qstash.http import HttpClient


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


def prepare_upsert_body(queue: str, parallelism: int) -> str:
    return json.dumps(
        {
            "queueName": queue,
            "parallelism": parallelism,
        }
    )


def parse_queue_response(response: Dict[str, Any]) -> Queue:
    return Queue(
        name=response["name"],
        parallelism=response["parallelism"],
        created_at=response["createdAt"],
        updated_at=response["updatedAt"],
        lag=response["lag"],
    )


class QueueApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def upsert(self, queue: str, *, parallelism: int = 1) -> None:
        """
        Updates or creates a queue.

        :param queue: The name of the queue.
        :param parallelism: The number of parallel consumers consuming from the queue.
        """
        body = prepare_upsert_body(queue, parallelism)

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
