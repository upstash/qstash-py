import json
from typing import List, Optional
from upstash_qstash.asyncio.http import AsyncHttpClient
from upstash_qstash.dlq import (
    DlqMessage,
    ListDlqMessagesResponse,
    parse_dlq_message_response,
)


class AsyncDlqApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def get(self, dlq_id: str) -> DlqMessage:
        """
        Gets a message from DLQ.

        :param dlq_id: The unique id within the DLQ to get.
        """
        response = await self._http.request(
            path=f"/v2/dlq/{dlq_id}",
            method="GET",
        )

        return parse_dlq_message_response(response, dlq_id)

    async def list(self, *, cursor: Optional[str] = None) -> ListDlqMessagesResponse:
        """
        Lists all messages currently inside the DLQ.

        :param cursor: Optional cursor to start listing DLQ messages from.
        """
        if cursor is not None:
            params = {"cursor": cursor}
        else:
            params = None

        response = await self._http.request(
            path="/v2/dlq",
            method="GET",
            params=params,
        )

        messages = [parse_dlq_message_response(r) for r in response["messages"]]

        return ListDlqMessagesResponse(
            cursor=response.get("cursor"),
            messages=messages,
        )

    async def delete(self, dlq_id: str) -> None:
        """
        Deletes a message from the DLQ.

        :param dlq_id: The unique id within the DLQ to delete.
        """
        await self._http.request(
            path=f"/v2/dlq/{dlq_id}",
            method="DELETE",
            parse_response=False,
        )

    async def delete_many(self, dlq_ids: List[str]) -> int:
        """
        Deletes multiple messages from the DLQ and
        returns how many of them are deleted.

        :param dlq_ids: The unique ids within the DLQ to delete.
        """
        body = json.dumps({"dlqIds": dlq_ids})

        response = await self._http.request(
            path="/v2/dlq",
            method="DELETE",
            headers={"Content-Type": "application/json"},
            body=body,
        )

        return response["deleted"]
