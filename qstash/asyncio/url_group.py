from typing import List

from qstash.asyncio.http import AsyncHttpClient
from qstash.url_group import (
    RemoveEndpointRequest,
    UpsertEndpointRequest,
    UrlGroup,
    parse_url_group_response,
    prepare_add_endpoints_body,
    prepare_remove_endpoints_body,
)


class AsyncUrlGroupApi:
    def __init__(self, http: AsyncHttpClient):
        self._http = http

    async def upsert_endpoints(
        self,
        url_group: str,
        endpoints: List[UpsertEndpointRequest],
    ) -> None:
        """
        Add or updates an endpoint to a url group.

        If the url group or the endpoint does not exist, it will be created.
        If the endpoint exists, it will be updated.
        """
        body = prepare_add_endpoints_body(endpoints)

        await self._http.request(
            path=f"/v2/topics/{url_group}/endpoints",
            method="POST",
            headers={"Content-Type": "application/json"},
            body=body,
            parse_response=False,
        )

    async def remove_endpoints(
        self,
        url_group: str,
        endpoints: List[RemoveEndpointRequest],
    ) -> None:
        """
        Remove one or more endpoints from a url group.

        If all endpoints have been removed, the url group will be deleted.
        """
        body = prepare_remove_endpoints_body(endpoints)

        await self._http.request(
            path=f"/v2/topics/{url_group}/endpoints",
            method="DELETE",
            headers={"Content-Type": "application/json"},
            body=body,
            parse_response=False,
        )

    async def get(self, url_group: str) -> UrlGroup:
        """
        Gets the url group by its name.
        """
        response = await self._http.request(
            path=f"/v2/topics/{url_group}",
            method="GET",
        )

        return parse_url_group_response(response)

    async def list(self) -> List[UrlGroup]:
        """
        Lists all the url groups.
        """
        response = await self._http.request(
            path="/v2/topics",
            method="GET",
        )

        return [parse_url_group_response(r) for r in response]

    async def delete(self, url_group: str) -> None:
        """
        Deletes the url group and all its endpoints.
        """
        await self._http.request(
            path=f"/v2/topics/{url_group}",
            method="DELETE",
            parse_response=False,
        )
