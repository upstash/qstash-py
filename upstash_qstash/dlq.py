import dataclasses
import json
from typing import Any, Dict, List, Optional

from upstash_qstash.http import HttpClient
from upstash_qstash.message import Message


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

    def list(self, *, cursor: Optional[str] = None) -> ListDlqMessagesResponse:
        """
        Lists all messages currently inside the DLQ.

        :param cursor: Optional cursor to start listing DLQ messages from.
        """
        if cursor is not None:
            params = {"cursor": cursor}
        else:
            params = None

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
