import json
from typing import List, Union

from upstash_qstash.publish import BatchRequest
from upstash_qstash.publish import Publish as SyncPublish
from upstash_qstash.publish import (
    PublishRequest,
    PublishToTopicResponse,
    PublishToUrlResponse,
)
from upstash_qstash.upstash_http import HttpClient


class Publish:
    @staticmethod
    async def publish_async(
        http: HttpClient, req: PublishRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Asynchronously publish a message to QStash.
        """
        SyncPublish._validate_request(req)
        headers = SyncPublish._prepare_headers(req)
        destination = SyncPublish._get_destination(req)

        return await http.request_async(
            {
                "path": ["v2", "publish", destination],
                "body": req.get("body"),
                "headers": headers,
                "method": "POST",
            }
        )

    @staticmethod
    async def publish_json_async(http: HttpClient, req: PublishRequest):
        """
        Asynchronously publish a message to QStash, automatically serializing the body as JSON.
        """
        if "body" in req:
            req["body"] = json.dumps(req["body"])

        req.setdefault("headers", {}).update({"Content-Type": "application/json"})

        return await Publish.publish_async(http, req)

    @staticmethod
    async def batch_async(
        http: HttpClient, req: BatchRequest
    ) -> List[Union[PublishToUrlResponse, PublishToTopicResponse]]:
        """
        Publish a batch of messages to QStash.
        """
        for message in req:
            SyncPublish._validate_request(message)
            message["headers"] = SyncPublish._prepare_headers(message)

        messages = []
        for message in req:
            destination = SyncPublish._get_destination(message)
            messages.append(
                {
                    "destination": destination,
                    "headers": message["headers"],
                    "body": message.get("body"),
                }
            )

        return await http.request_async(
            {
                "path": ["v2", "batch"],
                "body": json.dumps(messages),
                "headers": {
                    "Content-Type": "application/json",
                },
                "method": "POST",
            }
        )

    @staticmethod
    async def batch_json_async(
        http: HttpClient, req: BatchRequest
    ) -> List[Union[PublishToUrlResponse, PublishToTopicResponse]]:
        """
        Asynchronously publish a batch of messages to QStash, automatically serializing the body of each message into JSON.
        """
        for message in req:
            if "body" in message:
                message["body"] = json.dumps(message["body"])

            message.setdefault("headers", {}).update(
                {"Content-Type": "application/json"}
            )

        return await Publish.batch_async(http, req)
