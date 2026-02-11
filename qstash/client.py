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

    def readiness(self) -> str:
        """
        Checks if the QStash service is ready to accept requests.

        This endpoint is commonly used in orchestration systems (like Kubernetes
        readiness probes) to determine when a service is ready to receive traffic.

        :return: "OK" if the service is ready
        :raises QStashError: If the request fails
        """
        result: str = self.http.request(
            path="/v2/readiness",
            method="GET",
            parse_response=False,
        )
        return result
