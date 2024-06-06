import json
from typing import List, Optional, TypedDict, Union

from upstash_qstash.publish import (
    Publish,
    PublishRequest,
    PublishToTopicResponse,
    PublishToUrlResponse,
)
from upstash_qstash.upstash_http import HttpClient

UpsertQueueRequest = TypedDict(
    "UpsertQueueRequest",
    {
        "parallelism": int,
    },
)

QueueResponse = TypedDict(
    "QueueResponse",
    {
        "createdAt": int,
        "updatedAt": int,
        "name": str,
        "parallelism": int,
        "lag": int,
    },
)

EnqueueRequest = PublishRequest

QueueOpts = TypedDict(
    "QueueOpts",
    {
        "queue_name": Optional[str],
    },
)


class Queue:
    def __init__(self, http: HttpClient, queue_opts: Optional[QueueOpts] = None):
        self.http = http
        self.queue_name = queue_opts.get("queue_name") if queue_opts else None

    def upsert(self, req: UpsertQueueRequest):
        """
        Creates or updates a queue with the given name and parallelism
        """
        if not self.queue_name:
            raise ValueError("Please provide a queue name to the Queue constructor")

        body = {"queueName": self.queue_name, "parallelism": req["parallelism"]}

        self.http.request(
            {
                "method": "POST",
                "path": ["v2", "queues"],
                "headers": {
                    "Content-Type": "application/json",
                },
                "body": json.dumps(body),
                "parse_response_as_json": False,
            }
        )

    def get(self) -> QueueResponse:
        """
        Get the queue details
        """
        if not self.queue_name:
            raise ValueError("Please provide a queue name to the Queue constructor")

        return self.http.request(
            {
                "method": "GET",
                "path": ["v2", "queues", self.queue_name],
            }
        )

    def list(self) -> List[QueueResponse]:
        """
        List all queues
        """

        return self.http.request(
            {
                "method": "GET",
                "path": ["v2", "queues"],
            }
        )

    def delete(self):
        """
        Delete the queue
        """
        if not self.queue_name:
            raise ValueError("Please provide a queue name to the Queue constructor")

        return self.http.request(
            {
                "method": "DELETE",
                "path": ["v2", "queues", self.queue_name],
                "parse_response_as_json": False,
            }
        )

    def enqueue(
        self, req: EnqueueRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Enqueue a message to the queue
        """
        if not self.queue_name:
            raise ValueError("Please provide a queue name to the Queue constructor")

        Publish._validate_request(req)
        headers = Publish._prepare_headers(req)
        destination = Publish._get_destination(req)

        return self.http.request(
            {
                "path": [
                    "v2",
                    "enqueue",
                    self.queue_name,
                    destination,
                ],
                "body": req.get("body"),
                "headers": headers,
                "method": "POST",
            }
        )

    def enqueue_json(self, req: EnqueueRequest):
        """
        Enqueue a message to the queue with the body as JSON
        """
        if "body" in req:
            req["body"] = json.dumps(req["body"])

        req.setdefault("headers", {}).update({"Content-Type": "application/json"})

        return self.enqueue(req)
