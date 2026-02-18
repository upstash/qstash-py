from typing import Dict

from qstash.asyncio.http import AsyncHttpClient
from qstash.flow_control_api import (
    FlowControlInfo,
    GlobalParallelismInfo,
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

