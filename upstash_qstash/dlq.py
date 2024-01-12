from typing import Optional, TypedDict, List, Dict
from upstash_qstash.qstash_types import Method, UpstashRequest
from upstash_qstash.upstash_http import HttpClient

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
        "responseStatus": Optional[int],
        "responseHeader": Optional[str],
        "responseBody": Optional[str],
        "responseBodyBase64": Optional[str],
        "bodyBase64": Optional[str],
    },
)

ListMessagesOpts = TypedDict(
    "ListMessagesOpts",
    {
        "cursor": str,
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
        List messages in the dlq.

        Example:
        --------
        >>> dlq = client.dlq()
        >>> all_messages = []
        >>> cursor = None
        >>> while True:
        >>>     res = dlq.list_messages({"cursor": cursor})
        >>>     all_messages.extend(res["messages"])
        >>>     cursor = res.get("cursor")
        >>>     if cursor is None:
        >>>         break
        """
        req: UpstashRequest = {
            "path": ["v2", "dlq"],
            "method": "GET",
        }
        if opts is not None and opts.get("cursor") is not None:
            req["query"] = {"cursor": opts["cursor"]}

        return self.http.request(req)

    def get(self, dlq_message_id: str) -> DlqMessage:
        """
        Get a message from the dlq using its `dlqId`
        """
        return self.http.request(
            {
                "path": ["v2", "dlq", dlq_message_id],
                "method": "GET",
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
