from typing import List
from upstash_qstash.upstash_http import HttpClient
from upstash_qstash.topics import (
    AddEndpointsRequest,
    RemoveEndpointsRequest,
    Topic,
    Topics as SyncTopics,
)
from upstash_qstash.error import QstashException
import json


class Topics:
    def __init__(self, http: HttpClient):
        self.http = http

    async def add_endpoints(self, req: AddEndpointsRequest):
        """
        Asynchronously create a new topic with the given name and endpoints

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
        Asynchronously remove endpoints from a topic.

        :param req: An instance of RemoveEndpointsRequest containing the name and endpoints
        """
        self._validate_topic_request(req)
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
