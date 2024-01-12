from typing import Optional
from upstash_qstash.upstash_http import HttpClient
from upstash_qstash.dlq import ListMessagesOpts, ListMessageResponse
from upstash_qstash.qstash_types import UpstashRequest


class DLQ:
    def __init__(self, http: HttpClient):
        self.http = http

    async def list_messages(
        self, opts: Optional[ListMessagesOpts] = None
    ) -> ListMessageResponse:
        """
        Asynchronously list messages in the dlq.

        Example:
        --------
        >>> dlq = client.dlq()
        >>> all_events = []
        >>> cursor = None
        >>> while True:
        >>>     res = await dlq.list_messages({"cursor": cursor})
        >>>     all_events.extend(res["events"])
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

        return await self.http.request_async(req)

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
