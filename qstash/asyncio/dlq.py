import json
from typing import List, Optional

from qstash.asyncio.http import AsyncHttpClient
from qstash.dlq import (
    DlqMessage,
    ListDlqMessagesResponse,
    parse_dlq_message_response,
    DlqFilter,
    prepare_list_dlq_messages_params,
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

    async def list(
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
