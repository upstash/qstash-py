import dataclasses
from typing import Any, Dict

from qstash.http import HttpClient


@dataclasses.dataclass
class ReadinessResponse:
    ready: bool
    """Whether the QStash service is ready to accept requests."""


def parse_readiness_response(response: Dict[str, Any]) -> ReadinessResponse:
    return ReadinessResponse(
        ready=response["ready"],
    )


class ReadinessApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def check(self) -> ReadinessResponse:
        """
        Checks the readiness of the QStash service.
        """
        response = self._http.request(
            path="/v2/readiness",
            method="GET",
        )

        return parse_readiness_response(response)
