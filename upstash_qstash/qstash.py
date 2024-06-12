from typing import Optional, Union, Literal

from upstash_qstash.chat import ChatApi
from upstash_qstash.dlq import DlqApi
from upstash_qstash.event import EventApi
from upstash_qstash.http import RetryConfig, HttpClient
from upstash_qstash.message import MessageApi
from upstash_qstash.queue import QueueApi
from upstash_qstash.schedule import ScheduleApi
from upstash_qstash.signing_key import SigningKeyApi
from upstash_qstash.url_group import UrlGroupApi


class QStash:
    """Synchronous SDK for the Upstash QStash."""

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
        http = HttpClient(token, retry)
        self.message = MessageApi(http)
        """Message api."""

        self.url_group = UrlGroupApi(http)
        """Url group api."""

        self.queue = QueueApi(http)
        """Queue api."""

        self.schedule = ScheduleApi(http)
        """Schedule api."""

        self.signing_key = SigningKeyApi(http)
        """Signing key api."""

        self.event = EventApi(http)
        """Event api."""

        self.dlq = DlqApi(http)
        """Dlq (Dead Letter Queue) api."""

        self.chat = ChatApi(http)
        """Chat api."""
