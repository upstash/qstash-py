from typing import Optional, Dict, Any, TypedDict, List, Union
from upstash_qstash.qstash_types import Method, UpstashHeaders
from upstash_qstash.upstash_http import HttpClient
from upstash_qstash.error import QstashException
from upstash_qstash.utils import prefix_headers
import json

PublishRequest = TypedDict(
    "PublishRequest",
    {
        "url": str,
        "body": Any,
        "headers": Optional[Dict[Any, Any]],
        "delay": Optional[int],
        "not_before": Optional[int],
        "deduplication_id": Optional[str],
        "content_based_deduplication": Optional[bool],
        "retries": Optional[int],
        "callback": Optional[str],
        "failure_callback": Optional[str],
        "method": Optional[Method],
        "topic": Optional[str],
    },
)

PublishToUrlResponse = TypedDict(
    "PublishResponse",
    {"messageId": str, "deduplicated": Optional[bool]},
)

PublishToTopicSingleResponse = TypedDict(
    "PublishToTopicSingleResponse",
    {"messageId": str, "url": str, "deduplicated": Optional[bool]},
)

PublishToTopicResponse = List[PublishToTopicSingleResponse]


class Publish:
    @staticmethod
    def _validate_request(req: PublishRequest):
        """
        Validate the publish request to ensure it has either url or topic.
        """
        if (req.get("url") is None and req.get("topic") is None) or (
            req.get("url") is not None and req.get("topic") is not None
        ):
            raise QstashException(
                "Either 'url' or 'topic' must be provided, but not both."
            )

    @staticmethod
    def _prepare_headers(req: PublishRequest) -> UpstashHeaders:
        """
        Prepare and return headers for the publish request.
        """
        headers: UpstashHeaders = req.get("headers") or {}
        prefix_headers(headers)

        headers["Upstash-Method"] = req.get("method") or "POST"

        if req.get("delay") is not None:
            headers["Upstash-Delay"] = f"{req.get('delay')}s"

        if req.get("not_before") is not None:
            headers["Upstash-Not-Before"] = str(req.get("not_before"))

        if req.get("deduplication_id") is not None:
            headers["Upstash-Deduplication-Id"] = req.get("deduplication_id")

        if req.get("content_based_deduplication") is not None:
            headers["Upstash-Content-Based-Deduplication"] = "true"

        if req.get("retries") is not None:
            headers["Upstash-Retries"] = str(req.get("retries"))

        if req.get("callback") is not None:
            headers["Upstash-Callback"] = req.get("callback")

        if req.get("failure_callback") is not None:
            headers["Upstash-Failure-Callback"] = req.get("failure_callback")

        return headers

    @staticmethod
    def publish(
        http: HttpClient, req: PublishRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Internal implementation of publishing a message to QStash.
        """
        Publish._validate_request(req)
        headers = Publish._prepare_headers(req)

        res = http.request(
            {
                "path": ["v2", "publish", req.get("url") or req.get("topic")],
                "body": req.get("body"),
                "headers": headers,
                "method": "POST",
            }
        )

        return res

    @staticmethod
    def publish_json(http: HttpClient, req: PublishRequest):
        """
        Publish a message to QStash, automatically serializing the body as JSON.
        """
        if "body" in req:
            req["body"] = json.dumps(req["body"])

        req.setdefault("headers", {}).update({"Content-Type": "application/json"})

        return Publish.publish(http, req)

    @staticmethod
    async def publish_async(
        http: HttpClient, req: PublishRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Asynchronously publish a message to QStash.
        """
        Publish._validate_request(req)
        headers = Publish._prepare_headers(req)

        res = await http.request_async(
            {
                "path": ["v2", "publish", req.get("url") or req.get("topic")],
                "body": req.get("body"),
                "headers": headers,
                "method": "POST",
            }
        )

        return res

    @staticmethod
    async def publish_json_async(http: HttpClient, req: PublishRequest):
        """
        Asynchronously publish a message to QStash, automatically serializing the body as JSON.
        """
        if "body" in req:
            req["body"] = json.dumps(req["body"])

        req.setdefault("headers", {}).update({"Content-Type": "application/json"})

        return await Publish.publish_async(http, req)
