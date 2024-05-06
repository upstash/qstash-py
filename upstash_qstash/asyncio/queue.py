import json
from typing import List, Optional, Union

from upstash_qstash.publish import Publish, PublishToTopicResponse, PublishToUrlResponse
from upstash_qstash.queue import (
    EnqueueRequest,
    QueueOpts,
    QueueResponse,
    UpsertQueueRequest,
)
from upstash_qstash.upstash_http import HttpClient


class Queue:
    def __init__(self, http: HttpClient, queue_opts: Optional[QueueOpts] = None):
        self.http = http
        self.queue_name = queue_opts.get("queue_name") if queue_opts else None

    async def upsert(self, req: UpsertQueueRequest):
        """
        Asynchronously creates or updates a queue with the given name and parallelism
        """
        if not self.queue_name:
            raise ValueError("Please provide a queue name to the Queue constructor")

        body = {"queueName": self.queue_name, "parallelism": req["parallelism"]}

        await self.http.request_async(
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

    async def get(self) -> QueueResponse:
        """
        Asynchronously get the queue details
        """
        if not self.queue_name:
            raise ValueError("Please provide a queue name to the Queue constructor")

        return await self.http.request_async(
            {
                "method": "GET",
                "path": ["v2", "queues", self.queue_name],
            }
        )

    async def list(self) -> List[QueueResponse]:
        """
        Asynchronously list all queues
        """

        return await self.http.request_async(
            {
                "method": "GET",
                "path": ["v2", "queues"],
            }
        )

    async def delete(self):
        """
        Asynchronously delete the queue
        """
        if not self.queue_name:
            raise ValueError("Please provide a queue name to the Queue constructor")

        return await self.http.request_async(
            {
                "method": "DELETE",
                "path": ["v2", "queues", self.queue_name],
                "parse_response_as_json": False,
            }
        )

    async def enqueue(
        self, req: EnqueueRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Asynchronously enqueue a message to the queue
        """
        if not self.queue_name:
            raise ValueError("Please provide a queue name to the Queue constructor")

        Publish._validate_request(req)
        headers = Publish._prepare_headers(req)

        return await self.http.request_async(
            {
                "path": [
                    "v2",
                    "enqueue",
                    self.queue_name,
                    req.get("url") or req["topic"],
                ],
                "body": req.get("body"),
                "headers": headers,
                "method": "POST",
            }
        )

    async def enqueue_json(self, req: EnqueueRequest):
        """
        Asynchronously enqueue a message to the queue with the body as JSON
        """
        if "body" in req:
            req["body"] = json.dumps(req["body"])

        req.setdefault("headers", {}).update({"Content-Type": "application/json"})

        return await self.enqueue(req)
