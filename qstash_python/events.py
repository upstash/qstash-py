from typing import TypedDict, Optional, List, Dict
from enum import Enum
from upstash_http import HttpClient


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
        "cursor": Optional[int],
    },
)


class Events:
    @staticmethod
    def get(http: HttpClient, req: Optional[EventsRequest] = None) -> GetEventsResponse:
        """
        Retrieve logs.
        """
        query: Dict[str, int] = {}
        if req is not None and req.get("cursor") is not None and req["cursor"] > 0:
            query["cursor"] = req["cursor"]

        res = http.request(
            {
                "path": ["v2", "events"],
                "method": "GET",
                "query": query,
            }
        )
        return res
