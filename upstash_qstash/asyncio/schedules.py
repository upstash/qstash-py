from typing import List

from upstash_qstash.schedules import (
    CreateScheduleRequest,
    CreateScheduleResponse,
    Schedule,
)
from upstash_qstash.schedules import Schedules as SyncSchedules
from upstash_qstash.upstash_http import HttpClient


class Schedules:
    def __init__(self, http: HttpClient):
        self.http = http

    async def create(self, req: CreateScheduleRequest) -> CreateScheduleResponse:
        """
        Asynchronously create a new schedule with the specified parameters.
        :param req: A dictionary with the details of the schedule to create.
        :return: A dictionary containing the 'scheduleId' of the created schedule.
        :raises UpstashError: If required headers are missing.
        """
        SyncSchedules._validate_schedule_request(req)
        headers = SyncSchedules._prepare_headers(req)

        return await self.http.request_async(
            {
                "path": ["v2", "schedules", req["destination"]],
                "body": req.get("body"),
                "headers": headers,
                "method": "POST",
            }
        )

    async def get(self, scheduleId: str) -> Schedule:
        """
        Asynchronously retrieve a schedule by its ID.
        :param scheduleId: The unique identifier of the schedule to retrieve.
        :return: A dictionary representing the schedule details.
        """
        return await self.http.request_async(
            {
                "method": "GET",
                "path": ["v2", "schedules", scheduleId],
            }
        )

    async def list(self) -> List[Schedule]:
        """
        Asynchronously retrieve a list of all schedules.
        :return: A list of dictionaries, each representing a schedule's details.
        """
        return await self.http.request_async(
            {
                "path": ["v2", "schedules"],
                "method": "GET",
            }
        )

    async def delete(self, scheduleId: str) -> None:
        """
        Asynchronously delete a schedule by its ID.
        :param scheduleId: The unique identifier of the schedule to delete.
        """
        return await self.http.request_async(
            {
                "path": ["v2", "schedules", scheduleId],
                "method": "DELETE",
                "parse_response_as_json": False,
            }
        )
