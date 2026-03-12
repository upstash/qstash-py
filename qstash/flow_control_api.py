import dataclasses
from typing import Any, Dict, Optional, TypedDict

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

    is_paused: bool
    """Whether message delivery is paused for this flow control key."""

    is_pinned_parallelism: bool
    """Whether the parallelism configuration is pinned."""

    is_pinned_rate: bool
    """Whether the rate configuration is pinned."""


@dataclasses.dataclass
class GlobalParallelismInfo:
    """Information about global parallelism."""

    parallelism_max: int
    """The maximum global parallelism."""

    parallelism_count: int
    """The current number of active requests globally."""


class PinFlowControlOptions(TypedDict, total=False):
    """Options for pinning a flow control key configuration."""

    parallelism: int
    """The parallelism value to apply to the flow-control key."""

    rate: int
    """The rate value to apply to the flow-control key."""

    period: int
    """The period value to apply to the flow-control key, in seconds."""


class UnpinFlowControlOptions(TypedDict, total=False):
    """Options for unpinning a flow control key configuration."""

    parallelism: bool
    """Whether to unpin the parallelism configuration."""

    rate: bool
    """Whether to unpin the rate configuration."""


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
        is_paused=response.get("isPaused", False),
        is_pinned_parallelism=response.get("isPinnedParallelism", False),
        is_pinned_rate=response.get("isPinnedRate", False),
    )


class FlowControlApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

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

    def get_global_parallelism(self) -> GlobalParallelismInfo:
        """
        Gets the global parallelism info.
        """
        response = self._http.request(
            path="/v2/globalParallelism",
            method="GET",
        )

        return GlobalParallelismInfo(
            parallelism_max=response.get("parallelismMax", 0),
            parallelism_count=response.get("parallelismCount", 0),
        )

    def pause(self, flow_control_key: str) -> None:
        """
        Pause message delivery for a flow-control key.

        Messages already in the waitlist will remain there.
        New incoming messages will be added directly to the waitlist.

        :param flow_control_key: The flow control key to pause.
        """
        self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/pause",
            method="POST",
            parse_response=False,
        )

    def resume(self, flow_control_key: str) -> None:
        """
        Resume message delivery for a flow-control key.

        :param flow_control_key: The flow control key to resume.
        """
        self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/resume",
            method="POST",
            parse_response=False,
        )

    def pin(
        self, flow_control_key: str, options: Optional[PinFlowControlOptions] = None
    ) -> None:
        """
        Pin a processing configuration for a flow-control key.

        While pinned, the system ignores configurations provided by incoming
        messages and uses the pinned configuration instead.

        :param flow_control_key: The flow control key to pin.
        :param options: The configuration to pin.
        """
        params: Dict[str, str] = {}
        if options is not None:
            if "parallelism" in options:
                params["parallelism"] = str(options["parallelism"])
            if "rate" in options:
                params["rate"] = str(options["rate"])
            if "period" in options:
                params["period"] = str(options["period"])

        self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/pin",
            method="POST",
            params=params or None,
            parse_response=False,
        )

    def unpin(
        self, flow_control_key: str, options: Optional[UnpinFlowControlOptions] = None
    ) -> None:
        """
        Remove the pinned configuration for a flow-control key.

        After unpinning, the system resumes updating the configuration
        based on incoming messages.

        :param flow_control_key: The flow control key to unpin.
        :param options: Which configurations to unpin.
        """
        params: Dict[str, str] = {}
        if options is not None:
            if "parallelism" in options:
                params["parallelism"] = str(options["parallelism"]).lower()
            if "rate" in options:
                params["rate"] = str(options["rate"]).lower()

        self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/unpin",
            method="POST",
            params=params or None,
            parse_response=False,
        )

    def reset_rate(self, flow_control_key: str) -> None:
        """
        Reset the rate configuration state for a flow-control key.

        Clears the current rate count and immediately ends the current period.
        The current timestamp becomes the start of the new rate period.

        :param flow_control_key: The flow control key to reset rate for.
        """
        self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/resetRate",
            method="POST",
            parse_response=False,
        )
