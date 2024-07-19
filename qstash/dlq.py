import dataclasses
import json
from typing import Any, Dict, List, Optional, TypedDict

from qstash.http import HttpClient
from qstash.message import Message


@dataclasses.dataclass
class DlqMessage(Message):
    dlq_id: str
    """The unique id within the DLQ."""

    response_status: int
    """The HTTP status code of the last failed delivery attempt."""

    response_headers: Optional[Dict[str, List[str]]]
    """The response headers of the last failed delivery attempt."""

    response_body: Optional[str]
    """
    The response body of the last failed delivery attempt if it is
    composed of UTF-8 characters only, `None` otherwise.
    """

    response_body_base64: Optional[str]
    """
    The base64 encoded response body of the last failed delivery attempt
    if the response body contains non-UTF-8 characters, `None` otherwise.
    """


class DlqFilter(TypedDict, total=False):
    message_id: str
    """Filter DLQ entries by message id."""

    url: str
    """Filter DLQ entries by url."""

    url_group: str
    """Filter DLQ entries by url group name."""

    api: str
    """Filter DLQ entries by api name."""

    queue: str
    """Filter DLQ entries by queue name."""

    schedule_id: str
    """Filter DLQ entries by schedule id."""

    from_time: int
    """Filter DLQ entries by starting time, in milliseconds"""

    to_time: int
    """Filter DLQ entries by ending time, in milliseconds"""

    response_status: int
    """Filter DLQ entries by HTTP status of the response"""

    caller_ip: str
    """Filter DLQ entries by IP address of the publisher of the message"""


@dataclasses.dataclass
class ListDlqMessagesResponse:
    cursor: Optional[str]
    """
    A cursor which can be used in subsequent requests to paginate through
    all messages. If `None`, end of the DLQ messages are reached.
    """

    messages: List[DlqMessage]
    """List of DLQ messages."""


def parse_dlq_message_response(
    response: Dict[str, Any],
    dlq_id: str = "",
) -> DlqMessage:
    return DlqMessage(
        message_id=response["messageId"],
        url=response["url"],
        url_group=response.get("topicName"),
        endpoint=response.get("endpointName"),
        api=response.get("api"),
        queue=response.get("queueName"),
        body=response.get("body"),
        body_base64=response.get("bodyBase64"),
        method=response["method"],
        headers=response.get("header"),
        max_retries=response["maxRetries"],
        not_before=response["notBefore"],
        created_at=response["createdAt"],
        callback=response.get("callback"),
        failure_callback=response.get("failureCallback"),
        schedule_id=response.get("scheduleId"),
        caller_ip=response.get("callerIP"),
        dlq_id=response.get("dlqId", dlq_id),
        response_status=response["responseStatus"],
        response_headers=response.get("responseHeader"),
        response_body=response.get("responseBody"),
        response_body_base64=response.get("responseBodyBase64"),
    )


def prepare_list_dlq_messages_params(
    *,
    cursor: Optional[str],
    count: Optional[int],
    filter: Optional[DlqFilter],
) -> Dict[str, str]:
    params = {}

    if cursor is not None:
        params["cursor"] = cursor

    if count is not None:
        params["count"] = str(count)

    if filter is not None:
        if "message_id" in filter:
            params["messageId"] = filter["message_id"]

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

        if "response_status" in filter:
            params["responseStatus"] = str(filter["response_status"])

        if "caller_ip" in filter:
            params["callerIp"] = filter["caller_ip"]

    return params


class DlqApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def get(self, dlq_id: str) -> DlqMessage:
        """
        Gets a message from DLQ.

        :param dlq_id: The unique id within the DLQ to get.
        """
        response = self._http.request(
            path=f"/v2/dlq/{dlq_id}",
            method="GET",
        )

        return parse_dlq_message_response(response, dlq_id)

    def list(
        self,
        *,
        cursor: Optional[str] = None,
        count: Optional[int] = None,
        filter: Optional[DlqFilter] = None,
    ) -> ListDlqMessagesResponse:
        """
        Lists all messages currently inside the DLQ.

        :param cursor: Optional cursor to start listing DLQ messages from.
        :param count: The maximum number of DLQ messages to return.
            Default and max is `100`.
        :param filter: Filter to use.
        """
        params = prepare_list_dlq_messages_params(
            cursor=cursor,
            count=count,
            filter=filter,
        )

        response = self._http.request(
            path="/v2/dlq",
            method="GET",
            params=params,
        )

        messages = [parse_dlq_message_response(r) for r in response["messages"]]

        return ListDlqMessagesResponse(
            cursor=response.get("cursor"),
            messages=messages,
        )

    def delete(self, dlq_id: str) -> None:
        """
        Deletes a message from the DLQ.

        :param dlq_id: The unique id within the DLQ to delete.
        """
        self._http.request(
            path=f"/v2/dlq/{dlq_id}",
            method="DELETE",
            parse_response=False,
        )

    def delete_many(self, dlq_ids: List[str]) -> int:
        """
        Deletes multiple messages from the DLQ and
        returns how many of them are deleted.

        :param dlq_ids: The unique ids within the DLQ to delete.
        """
        body = json.dumps({"dlqIds": dlq_ids})

        response = self._http.request(
            path="/v2/dlq",
            method="DELETE",
            headers={"Content-Type": "application/json"},
            body=body,
        )

        return response["deleted"]
