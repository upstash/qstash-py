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
