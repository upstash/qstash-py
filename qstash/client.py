import dataclasses
from os import environ
from typing import Optional, Union, Literal

from qstash.dlq import DlqApi
from qstash.log import LogApi
from qstash.http import RetryConfig, HttpClient
from qstash.message import MessageApi
from qstash.queue import QueueApi
from qstash.schedule import ScheduleApi
from qstash.signing_key import SigningKeyApi
from qstash.url_group import UrlGroupApi


@dataclasses.dataclass
class ReadinessResponse:
    ready: bool
    """Whether QStash is ready to accept requests."""


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
            base_url or environ.get("QSTASH_URL"),
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

        self.log = LogApi(self.http)
        """Log api."""

        self.dlq = DlqApi(self.http)
        """Dlq (Dead Letter Queue) api."""

    def readiness(self) -> ReadinessResponse:
        """
        Checks the readiness of QStash.

        This endpoint can be used to check if QStash is ready to accept
        requests. It's useful for health checks and monitoring.

        :return: ReadinessResponse containing the ready status.
        """
        response = self.http.request(
            path="/v2/readiness",
            method="GET",
        )

        return ReadinessResponse(ready=response.get("ready", False))
