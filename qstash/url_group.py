import dataclasses
import json
from typing import Any, Dict, List, Optional, TypedDict

from qstash.errors import QStashError
from qstash.http import HttpClient


class UpsertEndpointRequest(TypedDict, total=False):
    url: str
    """The url of the endpoint"""

    name: str
    """The optional name of the endpoint"""


class RemoveEndpointRequest(TypedDict, total=False):
    url: str
    """The url of the endpoint"""

    name: str
    """The name of the endpoint"""


@dataclasses.dataclass
class Endpoint:
    url: str
    """The url of the endpoint"""

    name: Optional[str]
    """The name of the endpoint"""


@dataclasses.dataclass
class UrlGroup:
    name: str
    """The name of the url group."""

    created_at: int
    """The creation time of the url group, in unix milliseconds."""

    updated_at: int
    """The last update time of the url group, in unix milliseconds."""

    endpoints: List[Endpoint]
    """The list of endpoints."""


def prepare_add_endpoints_body(
    endpoints: List[UpsertEndpointRequest],
) -> str:
    for e in endpoints:
        if "url" not in e:
            raise QStashError("`url` of the endpoint must be provided.")

    return json.dumps(
        {
            "endpoints": endpoints,
        }
    )


def prepare_remove_endpoints_body(
    endpoints: List[RemoveEndpointRequest],
) -> str:
    for e in endpoints:
        if "url" not in e and "name" not in e:
            raise QStashError(
                "One of `url` or `name` of the endpoint must be provided."
            )

    return json.dumps(
        {
            "endpoints": endpoints,
        }
    )


def parse_url_group_response(response: Dict[str, Any]) -> UrlGroup:
    endpoints = []
    for e in response["endpoints"]:
        endpoints.append(
            Endpoint(
                url=e["url"],
                name=e.get("name"),
            )
        )

    return UrlGroup(
        name=response["name"],
        created_at=response["createdAt"],
        updated_at=response["updatedAt"],
        endpoints=endpoints,
    )


class UrlGroupApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def upsert_endpoints(
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

        self._http.request(
            path=f"/v2/topics/{url_group}/endpoints",
            method="POST",
            headers={"Content-Type": "application/json"},
            body=body,
            parse_response=False,
        )

    def remove_endpoints(
        self,
        url_group: str,
        endpoints: List[RemoveEndpointRequest],
    ) -> None:
        """
        Remove one or more endpoints from a url group.

        If all endpoints have been removed, the url group will be deleted.
        """
        body = prepare_remove_endpoints_body(endpoints)

        self._http.request(
            path=f"/v2/topics/{url_group}/endpoints",
            method="DELETE",
            headers={"Content-Type": "application/json"},
            body=body,
            parse_response=False,
        )

    def get(self, url_group: str) -> UrlGroup:
        """
        Gets the url group by its name.
        """
        response = self._http.request(
            path=f"/v2/topics/{url_group}",
            method="GET",
        )

        return parse_url_group_response(response)

    def list(self) -> List[UrlGroup]:
        """
        Lists all the url groups.
        """
        response = self._http.request(
            path="/v2/topics",
            method="GET",
        )

        return [parse_url_group_response(r) for r in response]

    def delete(self, url_group: str) -> None:
        """
        Deletes the url group and all its endpoints.
        """
        self._http.request(
            path=f"/v2/topics/{url_group}",
            method="DELETE",
            parse_response=False,
        )
