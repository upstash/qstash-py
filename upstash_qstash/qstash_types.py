from typing import Any, Callable, Dict, List, Literal, Optional, TypedDict, Union

Method = Union[
    Literal["GET"], Literal["POST"], Literal["PUT"], Literal["DELETE"], Literal["PATCH"]
]

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
    str,  # This allows for any other string as a key (Upstash-Forward-*)
]

UpstashHeaders = Dict[HeaderKey, Optional[Union[str, Method]]]

UpstashRequest = TypedDict(
    "UpstashRequest",
    {
        "path": List[str],
        "body": Any,
        "headers": UpstashHeaders,
        "method": Method,
        "query": Dict[str, str],
        "parse_response_as_json": bool,
    },
    total=False,
)

RetryConfig = TypedDict(
    "RetryConfig",
    {
        "attempts": int,
        "backoff": Callable,
    },
)
