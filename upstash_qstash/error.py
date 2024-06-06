import json
from typing import TypedDict

RateLimit = TypedDict(
    "RateLimit",
    {
        "limit": int,
        "remaining": int,
        "reset": int,
    },
)

ChatRateLimit = TypedDict(
    "ChatRateLimit",
    {
        "limit-requests": int,
        "limit-tokens": int,
        "remaining-requests": int,
        "remaining-tokens": int,
        "reset-requests": str,
        "reset-tokens": str,
    },
)


class QstashException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class QstashRateLimitException(QstashException):
    def __init__(self, args: RateLimit):
        super().__init__(f"You have been rate limited. {json.dumps(args)}")


class QstashChatRateLimitException(QstashException):
    def __init__(self, args: ChatRateLimit):
        super().__init__(f"You have been rate limited. {json.dumps(args)}")


class SignatureException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
