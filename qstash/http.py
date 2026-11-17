import math
import re
import time
from typing import TypedDict, Callable, Optional, Union, Literal, Any, Dict

import httpx

from qstash.errors import (
    RateLimitExceededError,
    QStashError,
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


_FORBIDDEN_IN_SEGMENT = re.compile(r"[/\\?#\x00-\x1f\x7f]")
_IGNORED_IN_DOT_SEGMENT = re.compile(r"[\s\x00-\x1f\x7f]")
_ENCODED_DOT = re.compile(r"%2e", re.IGNORECASE)


def _is_dot_segment(segment: str) -> bool:
    normalized = _ENCODED_DOT.sub(".", _IGNORED_IN_DOT_SEGMENT.sub("", segment))
    return normalized in (".", "..")


def path_segment(value: str, name: str = "Id") -> str:
    """Checks that an id can be used as a single segment of a request path."""
    if not isinstance(value, str):
        raise QStashError(f"{name} must be a string.")
    value = str.__str__(value)
    if not value:
        raise QStashError(f"{name} cannot be empty.")
    if _is_dot_segment(value):
        raise QStashError(f'{name} cannot be "." or "..".')
    if _FORBIDDEN_IN_SEGMENT.search(value) or value != value.strip():
        raise QStashError(
            f"{name} cannot contain '/', '\\', '?', '#' or control characters, "
            "or start or end with whitespace."
        )
    return value


def destination_path(destination: str) -> str:
    """Checks that a destination does not have "." or ".." path segments."""
    if not isinstance(destination, str):
        raise QStashError("Destination must be a string.")
    destination = str.__str__(destination)
    path = re.split(r"[?#]", destination, maxsplit=1)[0]
    if not path.strip():
        raise QStashError("Destination cannot be empty.")
    if any(_is_dot_segment(segment) for segment in re.split(r"[/\\]", path)):
        raise QStashError('Destination cannot have "." or ".." path segments.')
    return destination


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
        if "RateLimit-Limit" in headers:
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
        base_url: Optional[str] = None,
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

        self._base_url = base_url.rstrip("/") if base_url else BASE_URL

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
        base_url = base_url or self._base_url
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
        base_url = base_url or self._base_url
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
