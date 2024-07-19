import dataclasses
import enum
from typing import Any, Dict, List, Optional, TypedDict

from qstash.http import HttpClient


class EventState(enum.Enum):
    """The state of the message."""

    CREATED = "CREATED"
    """The message has been accepted and stored in QStash"""

    ACTIVE = "ACTIVE"
    """The task is currently being processed by a worker."""

    RETRY = "RETRY"
    """The task has been scheduled to retry."""

    ERROR = "ERROR"
    """
    The execution threw an error and the task is waiting to be retried
    or failed.
    """

    DELIVERED = "DELIVERED"
    """The message was successfully delivered."""

    FAILED = "FAILED"
    """
    The task has failed too many times or encountered an error that it
    cannot recover from.
    """

    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    """The cancel request from the user is recorded."""

    CANCELED = "CANCELED"
    """The cancel request from the user is honored."""


@dataclasses.dataclass
class Event:
    time: int
    """Timestamp of this log entry, in milliseconds"""

    message_id: str
    """The associated message id."""

    state: EventState
    """The current state of the message at this point in time."""

    error: Optional[str]
    """An explanation what went wrong."""

    next_delivery_time: Optional[int]
    """The next scheduled timestamp of the message, milliseconds."""

    url: str
    """The destination url."""

    url_group: Optional[str]
    """The name of the url group if this message was sent through a url group."""

    endpoint: Optional[str]
    """The name of the endpoint if this message was sent through a url group."""

    api: Optional[str]
    """The name of the api if this message was sent to an api."""

    queue: Optional[str]
    """The name of the queue if this message is enqueued on a queue."""

    schedule_id: Optional[str]
    """The schedule id of the message if the message is triggered by a schedule."""

    headers: Optional[Dict[str, List[str]]]
    """Headers of the message"""

    body_base64: Optional[str]
    """The base64 encoded body of the message."""


class EventFilter(TypedDict, total=False):
    message_id: str
    """Filter events by message id."""

    state: EventState
    """Filter events by state."""

    url: str
    """Filter events by url."""

    url_group: str
    """Filter events by url group name."""

    api: str
    """Filter events by api name."""

    queue: str
    """Filter events by queue name."""

    schedule_id: str
    """Filter events by schedule id."""

    from_time: int
    """Filter events by starting time, in milliseconds"""

    to_time: int
    """Filter events by ending time, in milliseconds"""


@dataclasses.dataclass
class ListEventsResponse:
    cursor: Optional[str]
    """
    A cursor which can be used in subsequent requests to paginate through 
    all events. If `None`, end of the events are reached.
    """

    events: List[Event]
    """List of events."""


def prepare_list_events_request_params(
    *,
    cursor: Optional[str],
    count: Optional[int],
    filter: Optional[EventFilter],
) -> Dict[str, str]:
    params = {}

    if cursor is not None:
        params["cursor"] = cursor

    if count is not None:
        params["count"] = str(count)

    if filter is not None:
        if "message_id" in filter:
            params["messageId"] = filter["message_id"]

        if "state" in filter:
            params["state"] = filter["state"].value

        if "url" in filter:
            params["url"] = filter["url"]

        if "url_group" in filter:
            params["topicName"] = filter["url_group"]

        if "api" in filter:
            params["api"] = filter["api"]

        if "queue" in filter:
            params["queueName"] = filter["queue"]

        if "schedule_id" in filter:
            params["scheduleId"] = filter["schedule_id"]

        if "from_time" in filter:
            params["fromDate"] = str(filter["from_time"])

        if "to_time" in filter:
            params["toDate"] = str(filter["to_time"])

    return params


def parse_events_response(response: List[Dict[str, Any]]) -> List[Event]:
    events = []

    for event in response:
        events.append(
            Event(
                time=event["time"],
                message_id=event["messageId"],
                state=EventState(event["state"]),
                error=event.get("error"),
                next_delivery_time=event.get("nextDeliveryTime"),
                url=event["url"],
                url_group=event.get("topicName"),
                endpoint=event.get("endpointName"),
                api=event.get("api"),
                queue=event.get("queueName"),
                schedule_id=event.get("scheduleId"),
                headers=event.get("header"),
                body_base64=event.get("body"),
            )
        )

    return events


class EventApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def list(
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

        response = self._http.request(
            path="/v2/events",
            method="GET",
            params=params,
        )

        events = parse_events_response(response["events"])

        return ListEventsResponse(
            cursor=response.get("cursor"),
            events=events,
        )
