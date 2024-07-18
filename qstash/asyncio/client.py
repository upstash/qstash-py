from typing import Literal, Optional, Union

from qstash.asyncio.chat import AsyncChatApi
from qstash.asyncio.dlq import AsyncDlqApi
from qstash.asyncio.event import AsyncEventApi
from qstash.asyncio.http import AsyncHttpClient
from qstash.asyncio.message import AsyncMessageApi
from qstash.asyncio.queue import AsyncQueueApi
from qstash.asyncio.schedule import AsyncScheduleApi
from qstash.asyncio.signing_key import AsyncSigningKeyApi
from qstash.asyncio.url_group import AsyncUrlGroupApi
from qstash.http import RetryConfig


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
