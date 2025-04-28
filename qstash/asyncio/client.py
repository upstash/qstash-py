from os import environ
from typing import Literal, Optional, Union

from qstash.asyncio.dlq import AsyncDlqApi
from qstash.asyncio.log import AsyncLogApi
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
        base_url: Optional[str] = None,
    ) -> None:
        """
        :param token: The authorization token from the Upstash console.
        :param retry: Configures how the client should retry requests.
        """
        self.http = AsyncHttpClient(
            token,
            retry,
            base_url or environ.get("QSTASH_URL"),
        )
        self.message = AsyncMessageApi(self.http)
        """Message api."""

        self.url_group = AsyncUrlGroupApi(self.http)
        """Url group api."""

        self.queue = AsyncQueueApi(self.http)
        """Queue api."""

        self.schedule = AsyncScheduleApi(self.http)
        """Schedule api."""

        self.signing_key = AsyncSigningKeyApi(self.http)
        """Signing key api."""

        self.log = AsyncLogApi(self.http)
        """Log api."""

        self.dlq = AsyncDlqApi(self.http)
        """Dlq (Dead Letter Queue) api."""
