from typing import Optional

from qstash.asyncio.http import AsyncHttpClient
from qstash.event import (
    EventFilter,
    ListEventsResponse,
    parse_events_response,
    prepare_list_events_request_params,
)


class AsyncEventApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def list(
        self,
        *,
        cursor: Optional[str] = None,
        count: Optional[int] = None,
        filter: Optional[EventFilter] = None,
    ) -> ListEventsResponse:
        """
        Lists all events that happened, such as message creation or delivery.

        :param cursor: Optional cursor to start listing events from.
        :param count: The maximum number of events to return.
            Default and max is `1000`.
        :param filter: Filter to use.
        """
        params = prepare_list_events_request_params(
            cursor=cursor,
            count=count,
            filter=filter,
        )

        response = await self._http.request(
            path="/v2/events",
            method="GET",
            params=params,
        )

        events = parse_events_response(response["events"])

        return ListEventsResponse(
            cursor=response.get("cursor"),
            events=events,
        )
