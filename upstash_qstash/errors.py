from typing import Optional


class QStashError(Exception): ...


class SignatureError(QStashError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class RateLimitExceededError(QStashError):
    def __init__(
        self, limit: Optional[str], remaining: Optional[str], reset: Optional[str]
    ):
        super().__init__(
            f"Exceeded burst rate limit: Limit: {limit}, remaining: {remaining}, reset: {reset}"
        )
        self.limit = limit
        self.remaining = remaining
        self.reset = reset


class DailyMessageLimitExceededError(QStashError):
    def __init__(
        self, limit: Optional[str], remaining: Optional[str], reset: Optional[str]
    ):
        super().__init__(
            f"Exceeded daily message limit: Limit: {limit}, remaining: {remaining}, reset: {reset}"
        )
        self.limit = limit
        self.remaining = remaining
        self.reset = reset


class ChatRateLimitExceededError(QStashError):
    def __init__(
        self,
        limit_requests: Optional[str],
        limit_tokens: Optional[str],
        remaining_requests: Optional[str],
        remaining_tokens: Optional[str],
        reset_requests: Optional[str],
        reset_tokens: Optional[str],
    ):
        super().__init__(
            f"Exceeded chat rate limit: "
            f"Request limit: {limit_requests}, remaining: {remaining_requests}, reset: {reset_requests}; "
            f"token limit: {limit_tokens}, remaining: {remaining_tokens}, reset: {reset_tokens}"
        )
        self.limit_requests = limit_requests
        self.limit_tokens = limit_tokens
        self.remaining_requests = remaining_requests
        self.remaining_tokens = remaining_tokens
        self.reset_requests = reset_requests
        self.reset_tokens = reset_tokens
