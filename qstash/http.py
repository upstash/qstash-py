import math
import time
from typing import TypedDict, Callable, Optional, Union, Literal, Any, Dict

import httpx

from qstash.errors import (
    RateLimitExceededError,
    QStashError,
    ChatRateLimitExceededError,
    DailyMessageLimitExceededError,
)


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


def chat_rate_limit_error(headers: httpx.Headers) -> ChatRateLimitExceededError:
    limit_requests = headers.get("x-ratelimit-limit-requests")
    limit_tokens = headers.get("x-ratelimit-limit-tokens")
    remaining_requests = headers.get("x-ratelimit-remaining-requests")
    remaining_tokens = headers.get("x-ratelimit-remaining-tokens")
    reset_requests = headers.get("x-ratelimit-reset-requests")
    reset_tokens = headers.get("x-ratelimit-reset-tokens")
    return ChatRateLimitExceededError(
        limit_requests=limit_requests,
        limit_tokens=limit_tokens,
        remaining_requests=remaining_requests,
        remaining_tokens=remaining_tokens,
        reset_requests=reset_requests,
        reset_tokens=reset_tokens,
    )


def daily_message_limit_error(headers: httpx.Headers) -> DailyMessageLimitExceededError:
    limit = headers.get("RateLimit-Limit")
    remaining = headers.get("RateLimit-Remaining")
    reset = headers.get("RateLimit-Reset")
    return DailyMessageLimitExceededError(
        limit=limit,
        remaining=remaining,
        reset=reset,
    )


def burst_rate_limit_error(headers: httpx.Headers) -> RateLimitExceededError:
    limit = headers.get("Burst-RateLimit-Limit")
    remaining = headers.get("Burst-RateLimit-Remaining")
    reset = headers.get("Burst-RateLimit-Reset")
    return RateLimitExceededError(
        limit=limit,
        remaining=remaining,
        reset=reset,
    )


def raise_for_non_ok_status(response: httpx.Response) -> None:
    if response.is_success:
        return

    if response.status_code == 429:
        headers = response.headers
        if "x-ratelimit-limit-requests" in headers:
            raise chat_rate_limit_error(headers)
        elif "RateLimit-Limit" in headers:
            raise daily_message_limit_error(headers)
        else:
            raise burst_rate_limit_error(headers)

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
        base_url: Optional[str] = None,
        token: Optional[str] = None,
    ) -> Any:
        base_url = base_url or BASE_URL
        token = token or self._token

        url = base_url + path
        headers = {"Authorization": token, **(headers or {})}

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
        base_url: Optional[str] = None,
        token: Optional[str] = None,
    ) -> httpx.Response:
        base_url = base_url or BASE_URL
        token = token or self._token

        url = base_url + path
        headers = {"Authorization": token, **(headers or {})}

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
