import requests
import math
from typing import Union
from qstash_types import UpstashRequest, RetryConfig

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

    def request(self, req: UpstashRequest) -> dict:
        """
        Sends an HTTP request.

        :param req: The request object.
        :return: The response object.
        """
        init_headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",  # TODO: Make this configurable, but set for publishJSON
        }

        headers = {**init_headers, **req.get("headers")}

        res = requests.post(
            url=f"{self.base_url}/{'/'.join(req.get('path'))}",
            headers=headers,
            json=req.get("body"),
        )

        print(res)
