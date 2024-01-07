import json
from typing import Union
from upstash_qstash.upstash_http import HttpClient
from upstash_qstash.publish import (
    PublishToUrlResponse,
    PublishToTopicResponse,
    PublishRequest,
)
from upstash_qstash.publish import Publish as SyncPublish


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

        return await http.request_async(
            {
                "path": ["v2", "publish", req.get("url") or req.get("topic")],
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
