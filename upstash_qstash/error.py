import json
from typing import TypedDict

RateLimitConfig = TypedDict(
    "RateLimitConfig",
    {
        "limit": int,
        "remaining": int,
        "reset": int,
    },
)


class QstashException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class QstashRateLimitException(QstashException):
    def __init__(self, args: RateLimitConfig):
        super().__init__(f"You have been rate limited. {json.dumps(args)}")


class SignatureException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
