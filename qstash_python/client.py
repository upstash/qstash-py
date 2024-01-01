import json
from typing import Optional, Union
from upstash_http import HttpClient
from qstash_types import (
    PublishRequest,
    UpstashHeaders,
    RetryConfig,
    PublishToUrlResponse,
    PublishToTopicResponse,
)
from error import QstashException
from utils import prefix_headers
from messages import Messages
from topics import Topics
from dlq import DLQ

DEFAULT_BASE_URL = "https://qstash.upstash.io"


class Client:
    def __init__(
        self,
        token: str,
        retry: Optional[Union[bool, RetryConfig]] = None,
        base_url: Optional[str] = DEFAULT_BASE_URL,
    ):
        self.http = HttpClient(token, retry, base_url)

    def publish(
        self, req: PublishRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Publish a message to QStash.

        If publishing to a URL (req contains 'url'), this method returns a PublishToUrlResponse:
        - PublishToUrlResponse: Contains 'messageId' indicating the unique ID of the message and
        an optional 'deduplicated' boolean indicating if the message is a duplicate.

        If publishing to a topic (req contains 'topic'), it returns a PublishToTopicResponse:
        - PublishToTopicResponse: Contains a list of PublishToTopicSingleResponse objects, each of which
        contains 'messageId' indicating the unique ID of the message, 'url' indicating the URL to which
        the message was published, and an optional 'deduplicated' boolean indicating if the message is a duplicate.

        :param req: An instance of PublishRequest containing the request details.
        :return: Response details including the message_id, url (if publishing to a topic),
                and possibly a deduplicated boolean. The exact return type depends on the publish target.
        :raises ValueError: If neither 'url' nor 'topic' is provided, or both are provided.
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

        res = self.http.request(
            {
                "path": ["v2", "publish", req.get("url") or req.get("topic")],
                "body": req.get("body"),
                "headers": headers,
                "method": "POST",
            }
        )

        return res

    def publish_json(
        self, req: PublishRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Publish a message to QStash, automatically serializing the body as JSON.

        :param req: An instance of PublishRequest containing the request details.
        :return: An instance of PublishResponse containing the response details.
        """
        headers: UpstashHeaders = req.get("headers", {})
        prefix_headers(headers)
        headers["Content-Type"] = "application/json"

        if "body" in req:
            req["body"] = json.dumps(req["body"])

        return self.publish(req)

    def messages(self):
        """
        Access the messages API.

        Read or cancel messages.
        """
        return Messages(self.http)

    def topics(self):
        """
        Access the topics API.

        Create, read, update, or delete topics.
        """
        return Topics(self.http)

    def dlq(self):
        """
        Access the dlq API.

        Read or remove messages from the DLQ.s
        """
        return DLQ(self.http)
