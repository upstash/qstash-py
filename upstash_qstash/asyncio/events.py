from typing import Optional

from upstash_qstash.events import EventsRequest, GetEventsResponse
from upstash_qstash.upstash_http import HttpClient


class Events:
    @staticmethod
    async def get(
        http: HttpClient, req: Optional[EventsRequest] = None
    ) -> GetEventsResponse:
        """
        Asynchronously retrieve logs.
        """
        query = {}
        if req is not None and req.get("cursor"):
            query["cursor"] = req["cursor"]

        return await http.request_async(
            {
                "path": ["v2", "events"],
                "method": "GET",
                "query": query,
            }
        )
