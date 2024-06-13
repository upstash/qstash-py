import math
import time
from typing import TypedDict, Callable, Optional, Union, Literal, Any, Dict

import httpx

from upstash_qstash.errors import RateLimitExceededError, QStashError


class RetryConfig(TypedDict, total=False):
    retries: int
    """Maximum number of retries will be performed after the initial request fails."""

    backoff: Callable[[int], float]
    """A function that returns how many milliseconds to backoff before the given retry attempt."""


DEFAULT_TIMEOUT = httpx.Timeout(
    timeout=600.0,
    connect=5.0,
)

DEFAULT_RETRY = RetryConfig(
    retries=5,
    backoff=lambda retry_count: math.exp(1 + retry_count) * 50,
)

NO_RETRY = RetryConfig(
    retries=0,
    backoff=lambda _: 0,
)

BASE_URL = "https://qstash.upstash.io"

HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]


def raise_for_non_ok_status(response: httpx.Response) -> None:
    if response.is_success:
        return

    if response.status_code == 429:
        limit = response.headers.get("Burst-RateLimit-Limit")
        remaining = response.headers.get("Burst-RateLimit-Remaining")
        reset = response.headers.get("Burst-RateLimit-Reset")
        raise RateLimitExceededError(limit, remaining, reset)

    raise QStashError(
        f"Request failed with status: {response.status_code}, body: {response.text}"
    )


class HttpClient:
    def __init__(
        self,
        token: str,
        retry: Optional[Union[Literal[False], RetryConfig]],
    ) -> None:
        self._token = f"Bearer {token}"

        if retry is None:
            self._retry = DEFAULT_RETRY
        elif retry is False:
            self._retry = NO_RETRY
        else:
            self._retry = retry

        self._client = httpx.Client(
            timeout=DEFAULT_TIMEOUT,
        )

    def request(
        self,
        *,
        path: str,
        method: HttpMethod,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Union[str, bytes]] = None,
        params: Optional[Dict[str, str]] = None,
        parse_response: bool = True,
    ) -> Any:
        url = BASE_URL + path
        headers = {"Authorization": self._token, **(headers or {})}

        max_attempts = 1 + max(0, self._retry["retries"])
        last_error = None
        response = None
        for attempt in range(max_attempts):
            try:
                response = self._client.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    content=body,
                )
                break  # Break the loop as soon as we receive a proper response
            except Exception as e:
                last_error = e
                backoff = self._retry["backoff"](attempt) / 1000
                time.sleep(backoff)

        if not response:
            # Can't be None at this point
            raise last_error  # type:ignore[misc]

        raise_for_non_ok_status(response)

        if parse_response:
            return response.json()

        return response.text

    def stream(
        self,
        *,
        path: str,
        method: HttpMethod,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Union[str, bytes]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        url = BASE_URL + path
        headers = {"Authorization": self._token, **(headers or {})}

        max_attempts = 1 + max(0, self._retry["retries"])
        last_error = None
        response = None
        for attempt in range(max_attempts):
            try:
                request = self._client.build_request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    content=body,
                )
                response = self._client.send(
                    request,
                    stream=True,
                )
                break  # Break the loop as soon as we receive a proper response
            except Exception as e:
                last_error = e
                backoff = self._retry["backoff"](attempt) / 1000
                time.sleep(backoff)

        if not response:
            # Can't be None at this point
            raise last_error  # type:ignore[misc]

        try:
            raise_for_non_ok_status(response)
        except Exception as e:
            response.close()
            raise e

        return response
