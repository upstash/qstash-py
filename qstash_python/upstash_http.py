import requests
import math
import time
from typing import Union
from qstash_types import UpstashRequest, RetryConfig
from error import QstashException, QstashRateLimitException
from urllib.parse import urlencode

NO_RETRY: RetryConfig = {"attempts": 1, "backoff": lambda _: 0}

DEFAULT_RETRY_CONFIG: RetryConfig = {
    "attempts": 6,  # 5 retries
    "backoff": lambda retry_count: math.exp(retry_count) * 50,
}


class HttpClient:
    def __init__(self, token: str, retry: Union[RetryConfig, bool], base_url: str):
        """
        Initializes the HttpClient.

        :param token: The authorization token from the upstash console.
        :param retry: The retry configuration object, which defines the retry behavior. False to disable retry.
        :param base_url: The base URL for the HTTP client. Trailing slashes are automatically removed.
        """
        self.base_url = base_url.rstrip("/")
        self.token = f"Bearer {token}"
        if retry is False:
            self.retry = NO_RETRY
        elif isinstance(retry, dict):
            self.retry = {**DEFAULT_RETRY_CONFIG, **retry}
        else:
            self.retry = DEFAULT_RETRY_CONFIG

    def request(self, req: UpstashRequest):
        """
        Sends an HTTP request.

        :param req: The request object.
        :return: The response object.
        """
        # Handle URL and query params
        url_parts = [self.base_url] + req.get("path", [])
        url = "/".join(s.strip("/") for s in url_parts)

        query_params = req.get("query", {})
        if query_params:
            url += "?" + urlencode(
                {k: v for k, v in query_params.items() if v is not None}
            )

        init_headers = {"Authorization": self.token}

        headers = {**init_headers, **req.get("headers", {})}

        error = None
        for i in range(self.retry["attempts"]):
            try:
                res = requests.request(
                    method=req.get("method", "GET"),
                    url=url,
                    headers=headers,
                    stream=req.get("keepalive", False),
                    data=req.get("body"),
                )

                if res.status_code == 429:
                    raise QstashRateLimitException(
                        {
                            "limit": res.headers.get("Burst-RateLimit-Limit"),
                            "remaining": res.headers.get("Burst-RateLimit-Remaining"),
                            "reset": res.headers.get("Burst-RateLimit-Reset"),
                        }
                    )
                if res.status_code < 200 or res.status_code >= 300:
                    raise QstashException(
                        f"Qstash request failed with status {res.status_code}: {res.text}"
                    )

                if req.get("parse_response_as_json", True):
                    return res.json()
                else:
                    return res.text
            except Exception as e:
                error = e
                time.sleep(self.retry["backoff"](i) / 1000)

        raise error or QstashException(
            "Exhausted all retries without a successful response"
        )
