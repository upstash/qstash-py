import dataclasses
from typing import Any, Dict, List, Optional

from qstash.http import HttpClient


@dataclasses.dataclass
class FlowControlInfo:
    """Information about a flow control key."""

    key: str
    """The flow control key."""

    wait_list_size: int
    """The number of messages waiting in the wait list."""

    parallelism_max: int
    """The maximum parallelism configured for this flow control key."""

    parallelism_count: int
    """The current number of active requests for this flow control key."""

    rate_max: int
    """The maximum rate configured for this flow control key."""

    rate_count: int
    """The current number of requests consumed in the current period."""

    rate_period: int
    """The rate period in seconds."""

    rate_period_start: int
    """The start time of the current rate period as a unix timestamp."""


def parse_flow_control_info(response: Dict[str, Any]) -> FlowControlInfo:
    return FlowControlInfo(
        key=response["flowControlKey"],
        wait_list_size=response.get("waitListSize", 0),
        parallelism_max=response.get("parallelismMax", 0),
        parallelism_count=response.get("parallelismCount", 0),
        rate_max=response.get("rateMax", 0),
        rate_count=response.get("rateCount", 0),
        rate_period=response.get("ratePeriod", 0),
        rate_period_start=response.get("ratePeriodStart", 0),
    )


class FlowControlApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def list(
        self,
        *,
        search: Optional[str] = None,
    ) -> List[FlowControlInfo]:
        """
        Lists all flow controls.

        :param search: Optional search string to filter flow control keys.
        """
        params: Dict[str, str] = {}
        if search is not None:
            params["search"] = search

        response = self._http.request(
            path="/v2/flowControl",
            method="GET",
            params=params,
        )

        return [parse_flow_control_info(r) for r in response]

    def get(self, flow_control_key: str) -> FlowControlInfo:
        """
        Gets a single flow control by key.

        :param flow_control_key: The flow control key to get.
        """
        response = self._http.request(
            path=f"/v2/flowControl/{flow_control_key}",
            method="GET",
        )

        return parse_flow_control_info(response)

    def reset(self, flow_control_key: str) -> None:
        """
        Resets the counters of a flow control key.

        :param flow_control_key: The flow control key to reset.
        """
        self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/reset",
            method="POST",
            parse_response=False,
        )
