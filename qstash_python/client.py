from typing import Optional, Union
from upstash_http import HttpClient
from qstash_types import PublishRequest, UpstashHeaders, RetryConfig
from utils import prefix_headers

DEFAULT_BASE_URL = "https://qstash.upstash.io"


class Client:
    def __init__(
        self,
        token: str,
        retry: Optional[Union[bool, RetryConfig]] = None,
        base_url: Optional[str] = DEFAULT_BASE_URL,
    ):
        self.http = HttpClient(token, retry, base_url)

    def publish(self, req: PublishRequest):
        """
        Publish a message to QStash.

        :param req: An instance of PublishRequest containing the request details.
        :return: An instance of PublishResponse containing the response details.
        """
        # Request should have either url or topic, but not both
        if (req.get("url") is None and req.get("topic") is None) or (
            req.get("url") is not None and req.get("topic") is not None
        ):
            raise ValueError("Either 'url' or 'topic' must be provided, but not both.")

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
