import dataclasses
import enum
from typing import Any, Dict, List, Optional, TypedDict

from qstash.http import HttpClient, HttpMethod
from qstash.message import parse_flow_control, FlowControlProperties


class LogState(enum.Enum):
    """State of the message."""

    CREATED = "CREATED"
    """Message has been accepted and stored in QStash"""

    ACTIVE = "ACTIVE"
    """Task is currently being processed by a worker."""

    RETRY = "RETRY"
    """Task has been scheduled to retry."""

    ERROR = "ERROR"
    """
    Execution threw an error and the task is waiting to be retried
    or failed.
    """

    DELIVERED = "DELIVERED"
    """Message was successfully delivered."""

    FAILED = "FAILED"
    """
    Task has failed too many times or encountered an error that it
    cannot recover from.
    """

    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    """Cancel request from the user is recorded."""

    CANCELED = "CANCELED"
    """Cancel request from the user is honored."""

    IN_PROGRESS = "IN_PROGRESS"
    """Messages which are in progress"""


@dataclasses.dataclass
class Log:
    time: int
    """Unix time of the log entry, in milliseconds."""

    message_id: str
    """Message id associated with the log."""

    state: LogState
    """Current state of the message at this point in time."""

    error: Optional[str]
    """An explanation what went wrong."""

    next_delivery_time: Optional[int]
    """Next scheduled Unix time of the message, milliseconds."""

    url: str
    """Destination url."""

    url_group: Optional[str]
    """Name of the url group if this message was sent through a url group."""

    endpoint: Optional[str]
    """Name of the endpoint if this message was sent through a url group."""

    queue: Optional[str]
    """Name of the queue if this message is enqueued on a queue."""

    schedule_id: Optional[str]
    """Schedule id of the message if the message is triggered by a schedule."""

    body_base64: Optional[str]
    """Base64 encoded body of the message."""

    headers: Optional[Dict[str, List[str]]]
    """Headers of the message"""

    callback_headers: Optional[Dict[str, List[str]]]
    """Headers of the callback message"""

    failure_callback_headers: Optional[Dict[str, List[str]]]
    """Headers of the failure callback message"""

    response_status: Optional[int]
    """HTTP status code of the last failed delivery attempt."""

    response_headers: Optional[Dict[str, List[str]]]
    """Response headers of the last failed delivery attempt."""

    response_body: Optional[str]
    """Response body of the last failed delivery attempt."""

    timeout: Optional[int]
    """HTTP timeout value used while calling the destination url."""

    method: Optional[HttpMethod]
    """HTTP method to use to deliver the message."""

    callback: Optional[str]
    """Url which is called each time the message is attempted to be delivered."""

    failure_callback: Optional[str]
    """Url which is called after the message is failed."""

    max_retries: Optional[int]
    """Number of retries that should be attempted in case of delivery failure."""

    retry_delay_expression: Optional[str]
    """
    The retry delay expression for this DLQ message,
    if retry_delay was set when publishing the message.
    """

    flow_control: Optional[FlowControlProperties]
    """Flow control properties"""


class LogFilter(TypedDict, total=False):
    message_id: str
    """Filter logs by message id."""

    message_ids: List[str]
    """Filter logs by message ids."""

    state: LogState
    """Filter logs by state."""

    url: str
    """Filter logs by url."""

    url_group: str
    """Filter logs by url group name."""

    queue: str
    """Filter logs by queue name."""

    schedule_id: str
    """Filter logs by schedule id."""

    from_time: int
    """Filter logs by starting Unix time, in milliseconds"""

    to_time: int
    """Filter logs by ending Unix time, in milliseconds"""


@dataclasses.dataclass
class ListLogsResponse:
    cursor: Optional[str]
    """
    A cursor which can be used in subsequent requests to paginate through 
    all logs. If `None`, end of the logs are reached.
    """

    logs: List[Log]
    """List of logs."""


def prepare_list_logs_request_params(
    *,
    cursor: Optional[str],
    count: Optional[int],
    filter: Optional[LogFilter],
) -> Dict[str, str]:
    params: Dict[str, Any] = {}

    if cursor is not None:
        params["cursor"] = cursor

    if count is not None:
        params["count"] = str(count)

    if filter is not None:
        if "message_id" in filter:
            params["messageId"] = filter["message_id"]

        if "message_ids" in filter:
            params["messageIds"] = filter["message_ids"]

        if "state" in filter:
            params["state"] = filter["state"].value

        if "url" in filter:
            params["url"] = filter["url"]

        if "url_group" in filter:
            params["topicName"] = filter["url_group"]

        if "queue" in filter:
            params["queueName"] = filter["queue"]

        if "schedule_id" in filter:
            params["scheduleId"] = filter["schedule_id"]

        if "from_time" in filter:
            params["fromDate"] = str(filter["from_time"])

        if "to_time" in filter:
            params["toDate"] = str(filter["to_time"])

    return params


def parse_logs_response(response: List[Dict[str, Any]]) -> List[Log]:
    logs = []

    for event in response:
        flow_control = parse_flow_control(event)
        logs.append(
            Log(
                time=event["time"],
                message_id=event["messageId"],
                state=LogState(event["state"]),
                error=event.get("error"),
                next_delivery_time=event.get("nextDeliveryTime"),
                url=event["url"],
                url_group=event.get("topicName"),
                endpoint=event.get("endpointName"),
                queue=event.get("queueName"),
                schedule_id=event.get("scheduleId"),
                headers=event.get("header"),
                callback_headers=event.get("callbackHeaders"),
                failure_callback_headers=event.get("failureCallbackHeaders"),
                body_base64=event.get("body"),
                response_status=event.get("responseStatus"),
                response_headers=event.get("responseHeader"),
                response_body=event.get("responseBody"),
                timeout=event.get("timeout"),
                callback=event.get("callback"),
                failure_callback=event.get("failureCallback"),
                flow_control=flow_control,
                method=event.get("method"),
                max_retries=event.get("maxRetries"),
                retry_delay_expression=event.get("retryDelayExpression"),
            )
        )

    return logs


class LogApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def list(
        self,
        *,
        cursor: Optional[str] = None,
        count: Optional[int] = None,
        filter: Optional[LogFilter] = None,
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

        response = self._http.request(
            path="/v2/events",
            method="GET",
            params=params,
        )

        logs = parse_logs_response(response["events"])

        return ListLogsResponse(
            cursor=response.get("cursor"),
            logs=logs,
        )
