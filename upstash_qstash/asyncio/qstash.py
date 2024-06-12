from typing import Literal, Optional, Union

from upstash_qstash.asyncio.chat import AsyncChatApi
from upstash_qstash.asyncio.dlq import AsyncDlqApi
from upstash_qstash.asyncio.event import AsyncEventApi
from upstash_qstash.asyncio.http import AsyncHttpClient
from upstash_qstash.asyncio.message import AsyncMessageApi
from upstash_qstash.asyncio.queue import AsyncQueueApi
from upstash_qstash.asyncio.schedule import AsyncScheduleApi
from upstash_qstash.asyncio.signing_key import AsyncSigningKeyApi
from upstash_qstash.asyncio.url_group import AsyncUrlGroupApi
from upstash_qstash.http import RetryConfig


class AsyncQStash:
    def __init__(
        self,
        token: str,
        *,
        retry: Optional[Union[Literal[False], RetryConfig]] = None,
    ) -> None:
        """
        :param token: The authorization token from the Upstash console.
        :param retry: Configures how the client should retry requests.
        """
        http = AsyncHttpClient(token, retry)
        self.message = AsyncMessageApi(http)
        """Message api."""

        self.url_group = AsyncUrlGroupApi(http)
        """Url group api."""

        self.queue = AsyncQueueApi(http)
        """Queue api."""

        self.schedule = AsyncScheduleApi(http)
        """Schedule api."""

        self.signing_key = AsyncSigningKeyApi(http)
        """Signing key api."""

        self.event = AsyncEventApi(http)
        """Event api."""

        self.dlq = AsyncDlqApi(http)
        """Dlq (Dead Letter Queue) api."""

        self.chat = AsyncChatApi(http)
        """Chat api."""
