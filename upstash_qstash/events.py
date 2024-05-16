from enum import Enum
from typing import List, Optional, TypedDict

from upstash_qstash.upstash_http import HttpClient


class State(Enum):
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    DELIVERED = "DELIVERED"
    ERROR = "ERROR"
    RETRY = "RETRY"
    FAILED = "FAILED"


Event = TypedDict(
    "Event",
    {
        "time": int,
        "state": State,
        "messageId": str,
        "nextDeliveryTime": Optional[int],
        "error": Optional[str],
        "url": str,
        "topicName": Optional[str],
        "endpointName": Optional[str],
    },
)

EventsRequest = TypedDict(
    "EventsRequest",
    {
        "cursor": str,
    },
)

GetEventsResponse = TypedDict(
    "GetEventsResponse",
    {
        "events": List[Event],
        "cursor": Optional[str],
    },
)


class Events:
    @staticmethod
    def get(http: HttpClient, req: Optional[EventsRequest] = None) -> GetEventsResponse:
        """
        Retrieve logs.
        """
        query = {}
        if req is not None and req.get("cursor") is not None:
            query["cursor"] = req["cursor"]

        return http.request(
            {
                "path": ["v2", "events"],
                "method": "GET",
                "query": query,
            }
        )
