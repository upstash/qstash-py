from typing import Optional, Dict, Any, List, TypedDict, Callable, Literal, Union
from enum import Enum


class Method(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"

Method = Union[Literal["GET"], Literal["POST"], str]

HeaderKey = Union[
    Literal["Upstash-Method"],
    Literal["Upstash-Delay"],
    Literal["Upstash-Not-Before"],
    Literal["Upstash-Deduplication-Id"],
    Literal["Upstash-Content-Based-Deduplication"],
    Literal["Upstash-Retries"],
    Literal["Upstash-Callback"],
    Literal["Upstash-Failure-Callback"],
    Literal["Upstash-Cron"],
    str  # This allows for any other string as a key (Upstash-Forward-*)
]

UpstashHeaders = Dict[HeaderKey, Optional[Union[str, Method]]]

UpstashRequest = TypedDict(
    "UpstashRequest",
    {
        "path": List[str],
        "body": Optional[Any],
        "headers": Optional[UpstashHeaders],
        "keepalive": Optional[bool],
        "method": Optional[Method],
        "query": Optional[Dict[str, str]],
        "parse_response_as_json": Optional[bool],
    },
)

RetryConfig = TypedDict(
    "RetryConfig",
    {
        "attempts": int,
        "backoff": Callable,
    },
)
