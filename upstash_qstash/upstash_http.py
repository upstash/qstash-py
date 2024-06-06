import asyncio
import json
import math
import time
from typing import Optional, Union
from urllib.parse import urlencode

import aiohttp
import requests

from upstash_qstash.error import (
    QstashChatRateLimitException,
    QstashException,
    QstashRateLimitException,
)
from upstash_qstash.qstash_types import RetryConfig, UpstashHeaders, UpstashRequest

NO_RETRY: RetryConfig = {"attempts": 1, "backoff": lambda _: 0}
DEFAULT_RETRY_CONFIG: RetryConfig = {
    "attempts": 6,  # 5 retries
    "backoff": lambda retry_count: math.exp(retry_count) * 50,
}


class HttpClient:
    def __init__(
        self, token: str, retry: Optional[Union[RetryConfig, bool]], base_url: str
    ):
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

    def _prepare_request_details(self, req: UpstashRequest):
        """
        Prepare the details for the request, handling any query parameters
        and headers that need to be added.

        :param req: The request to prepare details for.
        :return: The URL and headers for the request.
        """
        url_parts = [self.base_url] + req.get("path", [])
        url = "/".join(s.strip("/") for s in url_parts)
        query_params = req.get("query", {})
        if query_params:
            url += "?" + urlencode(
                {k: v for k, v in query_params.items() if v is not None}
            )

        init_headers = {"Authorization": self.token}
        headers: UpstashHeaders = {**init_headers, **req.get("headers", {})}

        return url, headers

    def request(self, req: UpstashRequest):
        """
        Synchronously make a request to QStash.

        :param req: The request to make.
        :return: The response from the request.
        """
        url, headers = self._prepare_request_details(req)
        error = None
        for i in range(self.retry["attempts"]):
            try:
                res = requests.request(
                    method=req["method"],
                    url=url,
                    headers=headers,
                    data=req.get("body"),
                )
                return self._handle_response(res, req)
            except Exception as e:
                error = e
                time.sleep(self.retry["backoff"](i) / 1000)
        raise error or QstashException(
            "Exhausted all retries without a successful response"
        )

    def request_stream(self, req: UpstashRequest):
        """
        Synchronously make a request to QStash, returning a generator that decodes
        SSE events until done message is received.

        :param req: The request to make.
        :return: The response from the request.
        """
        url, headers = self._prepare_request_details(req)
        error = None
        for i in range(self.retry["attempts"]):
            try:
                res = requests.request(
                    method=req["method"],
                    url=url,
                    headers=headers,
                    stream=True,
                    data=req.get("body"),
                )
                return self._handle_stream_response(res)
            except Exception as e:
                error = e
                time.sleep(self.retry["backoff"](i) / 1000)
        raise error or QstashException(
            "Exhausted all retries without a successful response"
        )

    def _check_status(self, res):
        if res.status_code == 429:
            if res.headers.get("x-ratelimit-limit-requests") is not None:
                raise QstashChatRateLimitException(
                    {
                        "limit-requests": res.headers.get("x-ratelimit-limit-requests"),
                        "limit-tokens": res.headers.get("x-ratelimit-limit-tokens"),
                        "remaining-requests": res.headers.get(
                            "x-ratelimit-remaining-requests"
                        ),
                        "remaining-tokens": res.headers.get(
                            "x-ratelimit-remaining-tokens"
                        ),
                        "reset-requests": res.headers.get("x-ratelimit-reset-requests"),
                        "reset-tokens": res.headers.get("x-ratelimit-reset-tokens"),
                    }
                )

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

    def _handle_response(self, res: requests.Response, req: UpstashRequest):
        """
        Synchronously handle the response from a request.
        Raises an exception if the response is not successful.
        If the response is successful, returns the response body.
        """
        self._check_status(res)

        return res.json() if req.get("parse_response_as_json", True) else res.text

    def _handle_stream_response(self, res: requests.Response):
        """
        Synchronously handle the response from a request in a streaming fashion
        until the done message is received.
        Raises an exception if the response is not successful.
        If the response is successful, returns a generator that yields response body in chunks.
        """
        try:
            self._check_status(res)

            for chunk in res.iter_lines(delimiter=b"\n\n"):
                if chunk.startswith(b"data: "):
                    chunk = chunk[6:]  # skip data header
                    if chunk == b"[DONE]":
                        break

                    yield json.loads(chunk)
        finally:
            res.close()

    async def request_async(self, req: UpstashRequest):
        """
        Asynchronously make a request to QStash.

        :param req: The request to make.
        :return: The response from the request.
        """
        url, headers = self._prepare_request_details(req)
        error = None
        for i in range(self.retry["attempts"]):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                        method=req["method"],
                        url=url,
                        headers=headers,
                        data=req.get("body"),
                    ) as res:
                        return await self._handle_response_async(res, req)
            except Exception as e:
                error = e
                await asyncio.sleep(self.retry["backoff"](i) / 1000)
        raise error or QstashException(
            "Exhausted all retries without a successful response"
        )

    async def request_stream_async(self, req: UpstashRequest):
        """
        Asynchronously make a request to QStash, returning a generator that decodes
        SSE events until done message is received.

        :param req: The request to make.
        :return: The response from the request.
        """
        url, headers = self._prepare_request_details(req)
        error = None
        for i in range(self.retry["attempts"]):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                        method=req["method"],
                        url=url,
                        headers=headers,
                        data=req.get("body"),
                    ) as res:
                        await self._check_status_async(res)
                        async for chunk in self._handle_stream_response_async(res):
                            yield chunk

                        return
            except Exception as e:
                error = e
                await asyncio.sleep(self.retry["backoff"](i) / 1000)
        raise error or QstashException(
            "Exhausted all retries without a successful response"
        )

    async def _check_status_async(self, res):
        if res.status == 429:
            headers = res.headers
            if headers.get("x-ratelimit-limit-requests") is not None:
                raise QstashChatRateLimitException(
                    {
                        "limit-requests": headers.get("x-ratelimit-limit-requests"),
                        "limit-tokens": headers.get("x-ratelimit-limit-tokens"),
                        "remaining-requests": headers.get(
                            "x-ratelimit-remaining-requests"
                        ),
                        "remaining-tokens": headers.get("x-ratelimit-remaining-tokens"),
                        "reset-requests": headers.get("x-ratelimit-reset-requests"),
                        "reset-tokens": headers.get("x-ratelimit-reset-tokens"),
                    }
                )

            raise QstashRateLimitException(
                {
                    "limit": headers.get("Burst-RateLimit-Limit"),
                    "remaining": headers.get("Burst-RateLimit-Remaining"),
                    "reset": headers.get("Burst-RateLimit-Reset"),
                }
            )

        if res.status < 200 or res.status >= 300:
            text = await res.text()
            raise QstashException(
                f"Qstash request failed with status {res.status}: {text}"
            )

    async def _handle_response_async(self, res, req: UpstashRequest):
        """
        Asynchronously handle the response from a request.
        Raises an exception if the response is not successful.
        If the response is successful, returns the response body.
        """
        await self._check_status_async(res)

        return (
            await res.json(content_type=None)
            if req.get("parse_response_as_json", True)
            else await res.text()
        )

    async def _handle_stream_response_async(self, res: aiohttp.ClientResponse):
        """
        Asynchronously handle the response from a request in a streaming fashion
        until the done message is received.
        Raises an exception if the response is not successful.
        If the response is successful, returns a generator that yields response body in chunks.
        """

        # Adapted from requests#iterlines
        pending = None
        async for data in res.content.iter_any():
            if pending is not None:
                data = pending + data

            chunks = data.split(b"\n\n")

            if chunks and chunks[-1] and data and chunks[-1][-1] == data[-1]:
                pending = chunks.pop()
            else:
                pending = None

            for chunk in chunks:
                if chunk.startswith(b"data: "):
                    chunk = chunk[6:]  # skip data header
                    if chunk == b"[DONE]":
                        return

                    yield json.loads(chunk)

        if pending is not None:
            if pending.startswith(b"data: "):
                pending = pending[6:]  # skip data header
                if pending == b"[DONE]":
                    return

                yield json.loads(pending)
