from typing import Optional, List, Dict, Any, TypedDict
from qstash_types import Method, UpstashHeaders
from upstash_http import HttpClient
from utils import prefix_headers
from error import QstashException
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
    def publish(http: HttpClient, req: PublishRequest):
        """
        Internal implementation of publishiong a message to QStash.
        """
        # Request should have either url or topic, but not both
        if (req.get("url") is None and req.get("topic") is None) or (
            req.get("url") is not None and req.get("topic") is not None
        ):
            raise QstashException(
                "Either 'url' or 'topic' must be provided, but not both."
            )

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
        headers: UpstashHeaders = req.get("headers", {})
        prefix_headers(headers)
        headers["Content-Type"] = "application/json"

        if "body" in req:
            req["body"] = json.dumps(req["body"])

        return Publish.publish(http, req)
