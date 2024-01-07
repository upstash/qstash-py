from upstash_qstash.messages import Message
from upstash_qstash.upstash_http import HttpClient


class Messages:
    def __init__(self, http: HttpClient):
        self.http = http

    async def get(self, messageId: str) -> Message:
        """
        Asynchronously get a message by ID.

        :param messageId: The ID of the message to get.
        :return: The message object.
        """
        return await self.http.request_async(
            {"path": ["v2", "messages", messageId], "method": "GET"}
        )

    async def delete(self, messageId: str):
        """
        Asynchronously cancel a message

        :param messageId: The ID of the message to cancel.
        """
        return await self.http.request_async(
            {
                "path": ["v2", "messages", messageId],
                "method": "DELETE",
                "parse_response_as_json": False,
            }
        )
