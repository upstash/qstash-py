import json
from typing import Any, Dict, List, Optional, Union

from qstash.asyncio.http import AsyncHttpClient
from qstash.http import HttpMethod
from qstash.schedule import (
    Schedule,
    parse_schedule_response,
    prepare_schedule_headers,
)


class AsyncScheduleApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def create(
        self,
        *,
        destination: str,
        cron: str,
        body: Optional[Union[str, bytes]] = None,
        content_type: Optional[str] = None,
        method: Optional[HttpMethod] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        callback: Optional[str] = None,
        failure_callback: Optional[str] = None,
        delay: Optional[Union[str, int]] = None,
        timeout: Optional[Union[str, int]] = None,
    ) -> str:
        """
        Creates a schedule to send messages periodically.

        Returns the created schedule id.

        :param destination: The destination url or url group.
        :param cron: The cron expression to use to schedule the messages.
        :param body: The raw request message body passed to the destination as is.
        :param content_type: MIME type of the message.
        :param method: The HTTP method to use when sending a webhook to your API.
        :param headers: Headers to forward along with the message.
        :param retries: How often should this message be retried in case the destination
            API is not available.
        :param callback: A callback url that will be called after each attempt.
        :param failure_callback: A failure callback url that will be called when a delivery
            is failed, that is when all the defined retries are exhausted.
        :param delay: Delay the message delivery. The format for the delay string is a
            number followed by duration abbreviation, like `10s`. Available durations
            are `s` (seconds), `m` (minutes), `h` (hours), and `d` (days). As convenience,
            it is also possible to specify the delay as an integer, which will be
            interpreted as delay in seconds.
        :param timeout: The HTTP timeout value to use while calling the destination URL.
            When a timeout is specified, it will be used instead of the maximum timeout
            value permitted by the QStash plan. It is useful in scenarios, where a message
            should be delivered with a shorter timeout.
        """
        req_headers = prepare_schedule_headers(
            cron=cron,
            content_type=content_type,
            method=method,
            headers=headers,
            retries=retries,
            callback=callback,
            failure_callback=failure_callback,
            delay=delay,
            timeout=timeout,
        )

        response = await self._http.request(
            path=f"/v2/schedules/{destination}",
            method="POST",
            headers=req_headers,
            body=body,
        )

        return response["scheduleId"]

    async def create_json(
        self,
        *,
        destination: str,
        cron: str,
        body: Optional[Any] = None,
        method: Optional[HttpMethod] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        callback: Optional[str] = None,
        failure_callback: Optional[str] = None,
        delay: Optional[Union[str, int]] = None,
        timeout: Optional[Union[str, int]] = None,
    ) -> str:
        """
        Creates a schedule to send messages periodically, automatically serializing the
        body as JSON string, and setting content type to `application/json`.

        Returns the created schedule id.

        :param destination: The destination url or url group.
        :param cron: The cron expression to use to schedule the messages.
        :param body: The request message body passed to the destination after being
            serialized as JSON string.
        :param method: The HTTP method to use when sending a webhook to your API.
        :param headers: Headers to forward along with the message.
        :param retries: How often should this message be retried in case the destination
            API is not available.
        :param callback: A callback url that will be called after each attempt.
        :param failure_callback: A failure callback url that will be called when a delivery
            is failed, that is when all the defined retries are exhausted.
        :param delay: Delay the message delivery. The format for the delay string is a
            number followed by duration abbreviation, like `10s`. Available durations
            are `s` (seconds), `m` (minutes), `h` (hours), and `d` (days). As convenience,
            it is also possible to specify the delay as an integer, which will be
            interpreted as delay in seconds.
        :param timeout: The HTTP timeout value to use while calling the destination URL.
            When a timeout is specified, it will be used instead of the maximum timeout
            value permitted by the QStash plan. It is useful in scenarios, where a message
            should be delivered with a shorter timeout.
        """
        return await self.create(
            destination=destination,
            cron=cron,
            body=json.dumps(body),
            content_type="application/json",
            method=method,
            headers=headers,
            retries=retries,
            callback=callback,
            failure_callback=failure_callback,
            delay=delay,
        )

    async def get(self, schedule_id: str) -> Schedule:
        """
        Gets the schedule by its id.
        """
        response = await self._http.request(
            path=f"/v2/schedules/{schedule_id}",
            method="GET",
        )

        return parse_schedule_response(response)

    async def list(self) -> List[Schedule]:
        """
        Lists all the schedules.
        """
        response = await self._http.request(
            path="/v2/schedules",
            method="GET",
        )

        return [parse_schedule_response(r) for r in response]

    async def delete(self, schedule_id: str) -> None:
        """
        Deletes the schedule.
        """
        await self._http.request(
            path=f"/v2/schedules/{schedule_id}",
            method="DELETE",
            parse_response=False,
        )

    async def pause(self, schedule_id: str) -> None:
        """
        Pauses the schedule.

        A paused schedule will not produce new messages until
        it is resumed.
        """
        await self._http.request(
            path=f"/v2/schedules/{schedule_id}/pause",
            method="PATCH",
            parse_response=False,
        )

    async def resume(self, schedule_id: str) -> None:
        """
        Resumes the schedule.
        """
        await self._http.request(
            path=f"/v2/schedules/{schedule_id}/resume",
            method="PATCH",
            parse_response=False,
        )
