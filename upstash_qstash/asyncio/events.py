from typing import Dict, Optional
from upstash_qstash.upstash_http import HttpClient
from upstash_qstash.events import State, Event, EventsRequest, GetEventsResponse


class Events:
    @staticmethod
    async def get(
        http: HttpClient, req: Optional[EventsRequest] = None
    ) -> GetEventsResponse:
        """
        Asynchronously retrieve logs.
        """
        query: Dict[str, int] = {}
        if req is not None and req.get("cursor") is not None and req["cursor"] > 0:
            query["cursor"] = req["cursor"]

        return await http.request(
            {
                "path": ["v2", "events"],
                "method": "GET",
                "query": query,
            }
        )
