from typing import Optional, TypedDict, List, Union
from upstash_http import HttpClient
from error import QstashException
import json

Endpoint = TypedDict(
    "Endpoint",
    {
        "name": Optional[str],
        "url": str,
    },
)

AddEndpointsRequest = TypedDict(
    "AddEndpointsRequest", {"name": str, "endpoints": List[Endpoint]}
)

RemoveEndpointVariant1 = TypedDict(
    "RemoveEndpointVariant1", {"name": str, "url": Optional[str]}, total=False
)

RemoveEndpointVariant2 = TypedDict(
    "RemoveEndpointVariant2", {"name": Optional[str], "url": str}, total=False
)

RemoveEndpoint = Union[RemoveEndpointVariant1, RemoveEndpointVariant2]

RemoveEndpointsRequest = TypedDict(
    "RemoveEndpointsRequest", {"name": str, "endpoints": List[RemoveEndpoint]}
)

Topic = TypedDict(
    "Topic",
    {
        "name": str,
        "endpoints": List[Endpoint],
    },
)


class Topics:
    def __init__(self, http: HttpClient):
        self.http = http

    def add_endpoints(self, req: AddEndpointsRequest):
        """
        Create a new topic with the given name and endpoints

        :param req: An instance of AddEndpointsRequest containing the name and endpoints
        """
        self._validate_topic_request(req)
        self.http.request(
            {
                "path": ["v2", "topics", req["name"], "endpoints"],
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"endpoints": req["endpoints"]}),
                "parse_response_as_json": False,
            }
        )

    def remove_endpoints(self, req: RemoveEndpointsRequest):
        """
        Remove endpoints from a topic.

        :param req: An instance of RemoveEndpointsRequest containing the name and endpoints
        """
        self._validate_topic_request(req)
        self.http.request(
            {
                "path": ["v2", "topics", req["name"], "endpoints"],
                "method": "DELETE",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"endpoints": req["endpoints"]}),
                "parse_response_as_json": False,
            }
        )

    def list(self) -> List[Topic]:
        """
        Get a list of all topics.

        :return: A list of Topic instances containing the topic details
        """
        return self.http.request(
            {
                "path": ["v2", "topics"],
                "method": "GET",
            }
        )

    def get(self, name: str) -> Topic:
        """
        Get a single topic

        :param name: The name of the topic
        :return: An instance of Topic containing the topic details
        """
        return self.http.request(
            {
                "path": ["v2", "topics", name],
                "method": "GET",
            }
        )

    def delete(self, name: str):
        """
        Delete a topic

        :param name: The name of the topic
        """
        return self.http.request(
            {
                "path": ["v2", "topics", name],
                "method": "DELETE",
                "parse_response_as_json": False,
            }
        )

    def _validate_topic_request(self, req: dict):
        """
        Ensure that the request contains a valid topic name and valid endpoints
        """
        if req.get("name") is None:
            raise QstashException("Topic name is required")
        if req.get("endpoints") is None:
            raise QstashException("Endpoints are required")
        for endpoint in req["endpoints"]:
            if endpoint.get("url") is None:
                raise QstashException("Endpoint url is required")
