from typing import List

import httpx
import pytest

from qstash.asyncio.http import AsyncHttpClient
from qstash.http import HttpClient


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("retries, expected_backoffs", [(0, []), (2, [0, 1])])
def test_no_backoff_after_last_attempt(
    stream: bool, retries: int, expected_backoffs: List[int]
) -> None:
    errors: List[httpx.ConnectError] = []
    backoffs: List[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        errors.append(httpx.ConnectError("Connection refused", request=request))
        raise errors[-1]

    def backoff(retry_count: int) -> float:
        backoffs.append(retry_count)
        return 0

    client = HttpClient(
        "token",
        retry={"retries": retries, "backoff": backoff},
        base_url="http://localhost:1",
    )
    client._client = httpx.Client(transport=httpx.MockTransport(handler))

    with pytest.raises(httpx.ConnectError) as exc_info:
        if stream:
            client.stream(path="/v2/test", method="GET")
        else:
            client.request(path="/v2/test", method="GET")

    assert len(errors) == retries + 1
    assert backoffs == expected_backoffs
    assert exc_info.value is errors[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("retries, expected_backoffs", [(0, []), (2, [0, 1])])
async def test_async_no_backoff_after_last_attempt(
    stream: bool, retries: int, expected_backoffs: List[int]
) -> None:
    errors: List[httpx.ConnectError] = []
    backoffs: List[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        errors.append(httpx.ConnectError("Connection refused", request=request))
        raise errors[-1]

    def backoff(retry_count: int) -> float:
        backoffs.append(retry_count)
        return 0

    client = AsyncHttpClient(
        "token",
        retry={"retries": retries, "backoff": backoff},
        base_url="http://localhost:1",
    )
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(httpx.ConnectError) as exc_info:
        if stream:
            await client.stream(path="/v2/test", method="GET")
        else:
            await client.request(path="/v2/test", method="GET")

    assert len(errors) == retries + 1
    assert backoffs == expected_backoffs
    assert exc_info.value is errors[-1]
