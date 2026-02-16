from typing import Dict, List, Optional

from qstash.asyncio.http import AsyncHttpClient
from qstash.flow_control_api import (
    FlowControlInfo,
    parse_flow_control_info,
)


class AsyncFlowControlApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def list(
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

        response = await self._http.request(
            path="/v2/flowControl",
            method="GET",
            params=params,
        )

        return [parse_flow_control_info(r) for r in response]

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

    async def reset(self, flow_control_key: str) -> None:
        """
        Resets the counters of a flow control key.

        :param flow_control_key: The flow control key to reset.
        """
        await self._http.request(
            path=f"/v2/flowControl/{flow_control_key}/reset",
            method="POST",
            parse_response=False,
        )
