from typing import Optional, Dict, Any, List, TypedDict
from enum import Enum


class Method(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


UpstashHeaders = TypedDict(
    "UpstashHeaders",
    {
        "Upstash-Method": Method,
        "Upstash-Delay": Optional[str],
        "Upstash-Not-Before": Optional[str],
        "Upstash-Deduplication-Id": Optional[str],
        "Upstash-Content-Based-Deduplication": Optional[str],
        "Upstash-Retries": Optional[str],
        "Upstash-Callback": Optional[str],
        "Upstash-Failure-Callback": Optional[str],
        "Upstash-Cron": Optional[str],
        # Other headers are prefixed with Upstash-Forward-
    },
)

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
        "backoff": callable,
    },
)
