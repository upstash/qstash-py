from typing import Optional

from qstash.asyncio.http import AsyncHttpClient
from qstash.log import (
    EventFilter,
    ListLogsResponse,
    parse_logs_response,
    prepare_list_logs_request_params,
)


class AsyncLogApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def list(
        self,
        *,
        cursor: Optional[str] = None,
        count: Optional[int] = None,
        filter: Optional[EventFilter] = None,
    ) -> ListLogsResponse:
        """
        Lists all logs that happened, such as message creation or delivery.

        :param cursor: Optional cursor to start listing logs from.
        :param count: The maximum number of logs to return.
            Default and max is `1000`.
        :param filter: Filter to use.
        """
        params = prepare_list_logs_request_params(
            cursor=cursor,
            count=count,
            filter=filter,
        )

        response = await self._http.request(
            path="/v2/events",
            method="GET",
            params=params,
        )

        logs = parse_logs_response(response["events"])

        return ListLogsResponse(
            cursor=response.get("cursor"),
            logs=logs,
        )
