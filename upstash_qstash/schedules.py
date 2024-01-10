import json
from typing import TypedDict, List, Dict, Optional
from upstash_qstash.qstash_types import Method, UpstashHeaders
from upstash_qstash.upstash_http import HttpClient
from upstash_qstash.utils import prefix_headers
from upstash_qstash.error import QstashException

Schedule = TypedDict(
    "Schedule",
    {
        "scheduleId": str,
        "cron": str,
        "createdAt": int,
        "destination": str,
        "method": str,
        "header": Optional[Dict[str, List[str]]],
        "body": Optional[str],
        "retries": int,
        "delay": Optional[int],
        "callback": Optional[str],
        "failureCallback": Optional[str],
    },
)

CreateScheduleRequest = TypedDict(
    "CreateScheduleRequest",
    {
        "destination": str,
        "body": Optional[str],
        "headers": Optional[Dict[str, List[str]]],
        "delay": Optional[int],
        "retries": Optional[int],
        "callback": Optional[str],
        "failure_callback": Optional[str],
        "method": Optional[Method],
        "cron": str,
    },
)

CreateScheduleResponse = TypedDict(
    "CreateScheduleResponse",
    {
        "scheduleId": str,
    },
)


class Schedules:
    def __init__(self, http: HttpClient):
        self.http = http

    @staticmethod
    def _prepare_headers(req: CreateScheduleRequest) -> UpstashHeaders:
        init_headers = req.get("headers") or {}
        headers = prefix_headers(init_headers)

        headers["Upstash-Method"] = req.get("method") or "POST"

        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"
            if req.get("body") is not None:
                req["body"] = json.dumps(req.get("body"))

        if req.get("cron") is not None:
            headers["Upstash-Cron"] = req.get("cron")

        if req.get("method") is not None:
            headers["Upstash-Method"] = req.get("method")

        if req.get("delay") is not None:
            headers["Upstash-Delay"] = f"{req.get('delay')}s"

        if req.get("retries") is not None:
            headers["Upstash-Retries"] = str(req.get("retries"))

        if req.get("callback") is not None:
            headers["Upstash-Callback"] = req.get("callback")

        if req.get("failure_callback") is not None:
            headers["Upstash-Failure-Callback"] = req.get("failure_callback")

        return headers

    @staticmethod
    def _validate_schedule_request(req: CreateScheduleRequest) -> None:
        if req.get("cron") is None:
            raise QstashException("Cron is required")
        if req.get("destination") is None:
            raise QstashException("Destination is required")

    def create(self, req: CreateScheduleRequest) -> CreateScheduleResponse:
        """
        Create a new schedule with the specified parameters.
        :param req: A dictionary with the details of the schedule to create.
        :return: A dictionary containing the 'scheduleId' of the created schedule.
        :raises UpstashError: If required headers are missing.
        """
        Schedules._validate_schedule_request(req)
        headers = Schedules._prepare_headers(req)

        return self.http.request(
            {
                "path": ["v2", "schedules", req["destination"]],
                "body": req.get("body"),
                "headers": headers,
                "method": "POST",
            }
        )

    def get(self, scheduleId: str) -> Schedule:
        """
        Retrieve a schedule by its ID.
        :param scheduleId: The unique identifier of the schedule to retrieve.
        :return: A dictionary representing the schedule details.
        """
        return self.http.request(
            {
                "method": "GET",
                "path": ["v2", "schedules", scheduleId],
            }
        )

    def list(self) -> List[Schedule]:
        """
        Retrieve a list of all schedules.
        :return: A list of dictionaries, each representing a schedule's details.
        """
        return self.http.request(
            {
                "path": ["v2", "schedules"],
                "method": "GET",
            }
        )

    def delete(self, scheduleId: str) -> None:
        """
        Delete a schedule by its ID.
        :param scheduleId: The unique identifier of the schedule to delete.
        """
        return self.http.request(
            {
                "path": ["v2", "schedules", scheduleId],
                "method": "DELETE",
                "parse_response_as_json": False,
            }
        )
