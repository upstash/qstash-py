import json
from typing import List

from upstash_qstash.topics import AddEndpointsRequest, RemoveEndpointsRequest, Topic
from upstash_qstash.topics import Topics as SyncTopics
from upstash_qstash.upstash_http import HttpClient


class Topics:
    def __init__(self, http: HttpClient):
        self.http = http

    async def upsert_or_add_endpoints(self, req: AddEndpointsRequest):
        """
        Asynchronously adds endpoints to a topic. If the topic does not exist, it will be created.

        :param req: An instance of AddEndpointsRequest containing the name and endpoints
        """
        SyncTopics._validate_topic_request(req)
        await self.http.request_async(
            {
                "path": ["v2", "topics", req["name"], "endpoints"],
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"endpoints": req["endpoints"]}),
                "parse_response_as_json": False,
            }
        )

    async def remove_endpoints(self, req: RemoveEndpointsRequest):
        """
        Asynchronously remove endpoints from a topic. When all endpoints are removed, the topic will be deleted.

        :param req: An instance of RemoveEndpointsRequest containing the name and endpoints
        """
        SyncTopics._validate_topic_request(req)
        await self.http.request_async(
            {
                "path": ["v2", "topics", req["name"], "endpoints"],
                "method": "DELETE",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"endpoints": req["endpoints"]}),
                "parse_response_as_json": False,
            }
        )

    async def list(self) -> List[Topic]:
        """
        Asynchronously get a list of all topics.

        :return: A list of Topic instances containing the topic details
        """
        return await self.http.request_async(
            {
                "path": ["v2", "topics"],
                "method": "GET",
            }
        )

    async def get(self, name: str) -> Topic:
        """
        Asynchronously get a single topic

        :param name: The name of the topic
        :return: An instance of Topic containing the topic details
        """
        return await self.http.request_async(
            {
                "path": ["v2", "topics", name],
                "method": "GET",
            }
        )

    async def delete(self, name: str):
        """
        Asynchronously delete a topic

        :param name: The name of the topic
        """
        return await self.http.request_async(
            {
                "path": ["v2", "topics", name],
                "method": "DELETE",
                "parse_response_as_json": False,
            }
        )
