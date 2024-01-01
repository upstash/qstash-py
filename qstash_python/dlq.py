from typing import Optional, TypedDict, List, Dict
from qstash_types import Method
from upstash_http import HttpClient

DlqMessage = TypedDict(
    "DlqMessage",
    {
        "dlqId": str,
        "messageId": str,
        "topicName": Optional[str],
        "url": str,
        "method": Optional[Method],
        "header": Optional[Dict[str, List[str]]],
        "body": Optional[str],
        "maxRetries": Optional[int],
        "notBefore": Optional[int],
        "createdAt": int,
        "callback": Optional[str],
        "failureCallback": Optional[str],
    },
)

ListMessagesOpts = TypedDict(
    "ListMessagesOpts",
    {
        "cursor": Optional[str],
    },
)

ListMessageResponse = TypedDict(
    "ListMessageResponse",
    {
        "messages": List[DlqMessage],
        "cursor": Optional[str],
    },
)


class DLQ:
    def __init__(self, http: HttpClient):
        self.http = http

    def list_messages(
        self, opts: Optional[ListMessagesOpts] = None
    ) -> ListMessageResponse:
        """
        List messages in the dlq
        """
        cursor = opts.get("cursor") if opts else None
        return self.http.request(
            {
                "path": ["v2", "dlq"],
                "method": "GET",
                "query": {"cursor": cursor},
            }
        )

    def delete(self, dlq_message_id: str):
        """
        Remove a message from the dlq using its `dlqId`
        """
        return self.http.request(
            {
                "path": ["v2", "dlq", dlq_message_id],
                "method": "DELETE",
                "parse_response_as_json": False,
            }
        )
