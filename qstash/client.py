from typing import Optional, Union, Literal

from qstash.chat import ChatApi
from qstash.dlq import DlqApi
from qstash.event import EventApi
from qstash.http import RetryConfig, HttpClient
from qstash.message import MessageApi
from qstash.queue import QueueApi
from qstash.schedule import ScheduleApi
from qstash.signing_key import SigningKeyApi
from qstash.url_group import UrlGroupApi


class QStash:
    """Synchronous SDK for the Upstash QStash."""

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
        self.http = HttpClient(
            token,
            retry,
            base_url,
        )
        self.message = MessageApi(self.http)
        """Message api."""

        self.url_group = UrlGroupApi(self.http)
        """Url group api."""

        self.queue = QueueApi(self.http)
        """Queue api."""

        self.schedule = ScheduleApi(self.http)
        """Schedule api."""

        self.signing_key = SigningKeyApi(self.http)
        """Signing key api."""

        self.event = EventApi(self.http)
        """Event api."""

        self.dlq = DlqApi(self.http)
        """Dlq (Dead Letter Queue) api."""

        self.chat = ChatApi(self.http)
        """Chat api."""
