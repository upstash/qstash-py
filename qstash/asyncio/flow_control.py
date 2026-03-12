from typing import Dict, Optional

from qstash.asyncio.http import AsyncHttpClient
from qstash.flow_control_api import (
    FlowControlInfo,
    GlobalParallelismInfo,
    PinFlowControlOptions,
    UnpinFlowControlOptions,
    parse_flow_control_info,
)


class AsyncFlowControlApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def get(self, flow_control_key: str) -> FlowControlInfo:
        """
        Gets a single flow control by key.

        :param flow_control_key: The flow control key to get.
        """
        response = await self._http.request(
            path=f"/v2/flowControl/{flow_control_key}",
            method="GET",
        )

        return parse_flow_control_info(response)

    async def get_global_parallelism(self) -> GlobalParallelismInfo:
        """
        Gets the global parallelism info.
        """
        response = await self._http.request(
            path="/v2/globalParallelism",
            method="GET",
        )

        return GlobalParallelismInfo(
            parallelism_max=response.get("parallelismMax", 0),
            parallelism_count=response.get("parallelismCount", 0),
        )

    async def pause(self, flow_control_key: str) -> None:
        """
        Pause message delivery for a flow-control key.

        Messages already in the waitlist will remain there.
        New incoming messages will be added directly to the waitlist.

        :param flow_control_key: The flow control key to pause.
        """
        await self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/pause",
            method="POST",
            parse_response=False,
        )

    async def resume(self, flow_control_key: str) -> None:
        """
        Resume message delivery for a flow-control key.

        :param flow_control_key: The flow control key to resume.
        """
        await self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/resume",
            method="POST",
            parse_response=False,
        )

    async def pin(
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
            if options.parallelism is not None:
                params["parallelism"] = str(options.parallelism)
            if options.rate is not None:
                params["rate"] = str(options.rate)
            if options.period is not None:
                params["period"] = str(options.period)

        await self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/pin",
            method="POST",
            params=params or None,
            parse_response=False,
        )

    async def unpin(
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
            if options.parallelism is not None:
                params["parallelism"] = str(options.parallelism).lower()
            if options.rate is not None:
                params["rate"] = str(options.rate).lower()

        await self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/unpin",
            method="POST",
            params=params or None,
            parse_response=False,
        )

    async def reset_rate(self, flow_control_key: str) -> None:
        """
        Reset the rate configuration state for a flow-control key.

        Clears the current rate count and immediately ends the current period.
        The current timestamp becomes the start of the new rate period.

        :param flow_control_key: The flow control key to reset rate for.
        """
        await self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/resetRate",
            method="POST",
            parse_response=False,
        )
