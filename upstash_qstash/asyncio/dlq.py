from typing import Optional
from upstash_qstash.upstash_http import HttpClient
from upstash_qstash.dlq import ListMessagesOpts, ListMessageResponse


class DLQ:
    def __init__(self, http: HttpClient):
        self.http = http

    async def list_messages(
        self, opts: Optional[ListMessagesOpts] = None
    ) -> ListMessageResponse:
        """
        Asynchronously list messages in the dlq
        """
        cursor = opts.get("cursor") if opts else None
        return await self.http.request_async(
            {
                "path": ["v2", "dlq"],
                "method": "GET",
                "query": {"cursor": cursor},
            }
        )

    async def delete(self, dlq_message_id: str):
        """
        Asynchronously remove a message from the dlq using its `dlqId`
        """
        return await self.http.request_async(
            {
                "path": ["v2", "dlq", dlq_message_id],
                "method": "DELETE",
                "parse_response_as_json": False,
            }
        )
