import dataclasses
import json
from typing import Any, Dict, List, Optional, Union

from qstash.http import HttpClient, HttpMethod


@dataclasses.dataclass
class Schedule:
    schedule_id: str
    """The id of the schedule."""

    destination: str
    """The destination url or url group."""

    cron: str
    """The cron expression used to schedule the messages."""

    created_at: int
    """The creation time of the schedule, in unix milliseconds."""

    body: Optional[str]
    """The body of the scheduled message if it is composed of UTF-8 characters only, 
    `None` otherwise.."""

    body_base64: Optional[str]
    """
    The base64 encoded body if the scheduled message body contains non-UTF-8 characters, 
    `None` otherwise.
    """

    method: HttpMethod
    """The HTTP method to use for the message."""

    headers: Optional[Dict[str, List[str]]]
    """The headers of the message."""

    retries: int
    """The number of retries that should be attempted in case of delivery failure."""

    callback: Optional[str]
    """The url which is called each time the message is attempted to be delivered."""

    failure_callback: Optional[str]
    """The url which is called after the message is failed."""

    delay: Optional[int]
    """The delay in seconds before the message is delivered."""

    caller_ip: Optional[str]
    """IP address of the creator of this schedule."""

    paused: bool
    """Whether the schedule is paused or not."""


def prepare_schedule_headers(
    *,
    cron: str,
    content_type: Optional[str],
    method: Optional[HttpMethod],
    headers: Optional[Dict[str, str]],
    retries: Optional[int],
    callback: Optional[str],
    failure_callback: Optional[str],
    delay: Optional[Union[str, int]],
    timeout: Optional[Union[str, int]],
) -> Dict[str, str]:
    h = {
        "Upstash-Cron": cron,
    }

    if content_type is not None:
        h["Content-Type"] = content_type

    if method is not None:
        h["Upstash-Method"] = method

    if headers:
        for k, v in headers.items():
            if not k.lower().startswith("upstash-forward-"):
                k = f"Upstash-Forward-{k}"

            h[k] = v

    if retries is not None:
        h["Upstash-Retries"] = str(retries)

    if callback is not None:
        h["Upstash-Callback"] = callback

    if failure_callback is not None:
        h["Upstash-Failure-Callback"] = failure_callback

    if delay is not None:
        if isinstance(delay, int):
            h["Upstash-Delay"] = f"{delay}s"
        else:
            h["Upstash-Delay"] = delay

    if timeout is not None:
        if isinstance(timeout, int):
            h["Upstash-Timeout"] = f"{timeout}s"
        else:
            h["Upstash-Timeout"] = timeout

    return h


def parse_schedule_response(response: Dict[str, Any]) -> Schedule:
    return Schedule(
        schedule_id=response["scheduleId"],
        destination=response["destination"],
        cron=response["cron"],
        created_at=response["createdAt"],
        body=response.get("body"),
        body_base64=response.get("bodyBase64"),
        method=response["method"],
        headers=response.get("header"),
        retries=response["retries"],
        callback=response.get("callback"),
        failure_callback=response.get("failureCallback"),
        delay=response.get("delay"),
        caller_ip=response.get("callerIP"),
        paused=response.get("isPaused", False),
    )


class ScheduleApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def create(
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

        response = self._http.request(
            path=f"/v2/schedules/{destination}",
            method="POST",
            headers=req_headers,
            body=body,
        )

        return response["scheduleId"]

    def create_json(
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
        return self.create(
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
            timeout=timeout,
        )

    def get(self, schedule_id: str) -> Schedule:
        """
        Gets the schedule by its id.
        """
        response = self._http.request(
            path=f"/v2/schedules/{schedule_id}",
            method="GET",
        )

        return parse_schedule_response(response)

    def list(self) -> List[Schedule]:
        """
        Lists all the schedules.
        """
        response = self._http.request(
            path="/v2/schedules",
            method="GET",
        )

        return [parse_schedule_response(r) for r in response]

    def delete(self, schedule_id: str) -> None:
        """
        Deletes the schedule.
        """
        self._http.request(
            path=f"/v2/schedules/{schedule_id}",
            method="DELETE",
            parse_response=False,
        )

    def pause(self, schedule_id: str) -> None:
        """
        Pauses the schedule.

        A paused schedule will not produce new messages until
        it is resumed.
        """
        self._http.request(
            path=f"/v2/schedules/{schedule_id}/pause",
            method="PATCH",
            parse_response=False,
        )

    def resume(self, schedule_id: str) -> None:
        """
        Resumes the schedule.
        """
        self._http.request(
            path=f"/v2/schedules/{schedule_id}/resume",
            method="PATCH",
            parse_response=False,
        )
