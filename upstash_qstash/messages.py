from typing import TypedDict, Optional, Dict, List
from upstash_qstash.qstash_types import Method
from upstash_qstash.upstash_http import HttpClient

Message = TypedDict(
    "Message",
    {
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


class Messages:
    def __init__(self, http: HttpClient):
        self.http = http

    def get(self, messageId: str) -> Message:
        """
        Get a message by ID.

        :param messageId: The ID of the message to get.
        :return: The message object.
        """
        return self.http.request(
            {"path": ["v2", "messages", messageId], "method": "GET"}
        )

    def delete(self, messageId: str):
        """
        Cancel a message

        :param messageId: The ID of the message to cancel.
        """
        return self.http.request(
            {
                "path": ["v2", "messages", messageId],
                "method": "DELETE",
                "parse_response_as_json": False,
            }
        )
