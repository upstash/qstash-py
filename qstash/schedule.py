import dataclasses
import enum
import json
from typing import Any, Dict, List, Optional, Union

from qstash.http import HttpClient, HttpMethod
from qstash.message import FlowControl, parse_flow_control, FlowControlProperties


class ScheduleState(enum.Enum):
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"


@dataclasses.dataclass
class Schedule:
    schedule_id: str
    """Id of the schedule."""

    destination: str
    """Destination url or url group."""

    cron: str
    """Cron expression used to schedule the messages."""

    created_at: int
    """Unix time in milliseconds when the schedule was created."""

    body: Optional[str]
    """Body of the scheduled message if it is composed of UTF-8 characters only."""

    body_base64: Optional[str]
    """Base64 encoded body if the scheduled message body contains non-UTF-8 characters."""

    method: HttpMethod
    """HTTP method to use to deliver the message."""

    headers: Optional[Dict[str, List[str]]]
    """Headers that will be forwarded to destination."""

    callback_headers: Optional[Dict[str, List[str]]]
    """Headers that will be forwarded to callback url."""

    failure_callback_headers: Optional[Dict[str, List[str]]]
    """Headers that will be forwarded to failure callback url."""

    retries: int
    """Number of retries that should be attempted in case of delivery failure."""

    callback: Optional[str]
    """Url which is called each time the message is attempted to be delivered."""

    failure_callback: Optional[str]
    """Url which is called after the message is failed."""

    queue: Optional[str]
    """
    Name of the queue which the messages will be enqueued, 
    if the destination is a queue.
    """

    delay: Optional[int]
    """Delay in seconds before the message is delivered."""

    timeout: Optional[int]
    """HTTP timeout value to use while calling the destination url."""

    caller_ip: Optional[str]
    """IP address of the creator of this schedule."""

    paused: bool
    """Whether the schedule is paused or not."""

    flow_control: Optional[FlowControlProperties]
    """Flow control properties"""

    last_schedule_time: Optional[int]
    """Unix time of the last schedule, in milliseconds."""

    next_schedule_time: Optional[int]
    """Unix time of the next schedule, in milliseconds."""

    last_schedule_states: Optional[Dict[str, ScheduleState]]
    """
    Map of the message ids to schedule states for the 
    published/enqueued messages in the last schedule run.
    """

    error: Optional[str]
    """Error message of the last schedule trigger."""


def prepare_schedule_headers(
    *,
    cron: str,
    content_type: Optional[str],
    method: Optional[HttpMethod],
    headers: Optional[Dict[str, str]],
    callback_headers: Optional[Dict[str, str]],
    failure_callback_headers: Optional[Dict[str, str]],
    retries: Optional[int],
    callback: Optional[str],
    failure_callback: Optional[str],
    delay: Optional[Union[str, int]],
    timeout: Optional[Union[str, int]],
    schedule_id: Optional[str],
    queue: Optional[str],
    flow_control: Optional[FlowControl],
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

            h[k] = str(v)

    if callback_headers:
        for k, v in callback_headers.items():
            if not k.lower().startswith("upstash-callback-"):
                k = f"Upstash-Callback-{k}"

            h[k] = str(v)

    if failure_callback_headers:
        for k, v in failure_callback_headers.items():
            if not k.lower().startswith("upstash-failure-callback-"):
                k = f"Upstash-Failure-Callback-{k}"

            h[k] = str(v)

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

    if schedule_id is not None:
        h["Upstash-Schedule-Id"] = schedule_id

    if flow_control and "key" in flow_control:
        control_values = []

        if "parallelism" in flow_control:
            control_values.append(f"parallelism={flow_control['parallelism']}")

        if "rate" in flow_control:
            control_values.append(f"rate={flow_control['rate']}")

        if "period" in flow_control:
            period = flow_control["period"]
            if isinstance(period, int):
                period = f"{period}s"

            control_values.append(f"period={period}")

        h["Upstash-Flow-Control-Key"] = flow_control["key"]
        h["Upstash-Flow-Control-Value"] = ", ".join(control_values)

    if queue is not None:
        h["Upstash-Queue-Name"] = queue

    return h


def parse_schedule_response(response: Dict[str, Any]) -> Schedule:
    flow_control = parse_flow_control(response)

    if "lastScheduleStates" in response:
        last_states = response["lastScheduleStates"]
        for k, v in last_states.items():
            last_states[k] = ScheduleState(v)
    else:
        last_states = None

    return Schedule(
        schedule_id=response["scheduleId"],
        destination=response["destination"],
        cron=response["cron"],
        created_at=response["createdAt"],
        body=response.get("body"),
        body_base64=response.get("bodyBase64"),
        method=response["method"],
        headers=response.get("header"),
        callback_headers=response.get("callbackHeader"),
        failure_callback_headers=response.get("failureCallbackHeader"),
        retries=response["retries"],
        callback=response.get("callback"),
        failure_callback=response.get("failureCallback"),
        delay=response.get("delay"),
        timeout=response.get("timeout"),
        caller_ip=response.get("callerIP"),
        paused=response.get("isPaused", False),
        queue=response.get("queueName"),
        flow_control=flow_control,
        last_schedule_time=response.get("lastScheduleTime"),
        next_schedule_time=response.get("nextScheduleTime"),
        last_schedule_states=last_states,
        error=response.get("error"),
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
        callback_headers: Optional[Dict[str, str]] = None,
        failure_callback_headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        callback: Optional[str] = None,
        failure_callback: Optional[str] = None,
        delay: Optional[Union[str, int]] = None,
        timeout: Optional[Union[str, int]] = None,
        schedule_id: Optional[str] = None,
        queue: Optional[str] = None,
        flow_control: Optional[FlowControl] = None,
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
        :param callback_headers: Headers to forward along with the callback message.
        :param failure_callback_headers: Headers to forward along with the failure \
            callback message.
        :param retries: How often should this message be retried in case the destination \
            API is not available.
        :param callback: A callback url that will be called after each attempt.
        :param failure_callback: A failure callback url that will be called when a delivery \
            is failed, that is when all the defined retries are exhausted.
        :param delay: Delay the message delivery. The format for the delay string is a \
            number followed by duration abbreviation, like `10s`. Available durations \
            are `s` (seconds), `m` (minutes), `h` (hours), and `d` (days). As convenience, \
            it is also possible to specify the delay as an integer, which will be \
            interpreted as delay in seconds.
        :param timeout: The HTTP timeout value to use while calling the destination URL. \
            When a timeout is specified, it will be used instead of the maximum timeout \
            value permitted by the QStash plan. It is useful in scenarios, where a message \
            should be delivered with a shorter timeout.
        :param schedule_id: Schedule id to use. This can be used to update the settings \
            of an existing schedule.
        :param queue: Name of the queue which the scheduled messages will be enqueued.
        :param flow_control: Settings for controlling the number of active requests, \
            as well as the rate of requests with the same flow control key.
        """
        req_headers = prepare_schedule_headers(
            cron=cron,
            content_type=content_type,
            method=method,
            headers=headers,
            callback_headers=callback_headers,
            failure_callback_headers=failure_callback_headers,
            retries=retries,
            callback=callback,
            failure_callback=failure_callback,
            delay=delay,
            timeout=timeout,
            schedule_id=schedule_id,
            queue=queue,
            flow_control=flow_control,
        )

        response = self._http.request(
            path=f"/v2/schedules/{destination}",
            method="POST",
            headers=req_headers,
            body=body,
        )

        return response["scheduleId"]  # type:ignore[no-any-return]

    def create_json(
        self,
        *,
        destination: str,
        cron: str,
        body: Optional[Any] = None,
        method: Optional[HttpMethod] = None,
        headers: Optional[Dict[str, str]] = None,
        callback_headers: Optional[Dict[str, str]] = None,
        failure_callback_headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        callback: Optional[str] = None,
        failure_callback: Optional[str] = None,
        delay: Optional[Union[str, int]] = None,
        timeout: Optional[Union[str, int]] = None,
        schedule_id: Optional[str] = None,
        queue: Optional[str] = None,
        flow_control: Optional[FlowControl] = None,
    ) -> str:
        """
        Creates a schedule to send messages periodically, automatically serializing the
        body as JSON string, and setting content type to `application/json`.

        Returns the created schedule id.

        :param destination: The destination url or url group.
        :param cron: The cron expression to use to schedule the messages.
        :param body: The request message body passed to the destination after being \
            serialized as JSON string.
        :param method: The HTTP method to use when sending a webhook to your API.
        :param headers: Headers to forward along with the message.
        :param callback_headers: Headers to forward along with the callback message.
        :param failure_callback_headers: Headers to forward along with the failure \
            callback message.
        :param retries: How often should this message be retried in case the destination \
            API is not available.
        :param callback: A callback url that will be called after each attempt.
        :param failure_callback: A failure callback url that will be called when a delivery \
            is failed, that is when all the defined retries are exhausted.
        :param delay: Delay the message delivery. The format for the delay string is a \
            number followed by duration abbreviation, like `10s`. Available durations \
            are `s` (seconds), `m` (minutes), `h` (hours), and `d` (days). As convenience, \
            it is also possible to specify the delay as an integer, which will be
            interpreted as delay in seconds.
        :param timeout: The HTTP timeout value to use while calling the destination URL.
            When a timeout is specified, it will be used instead of the maximum timeout
            value permitted by the QStash plan. It is useful in scenarios, where a message
            should be delivered with a shorter timeout.
        :param schedule_id: Schedule id to use. This can be used to update the settings \
            of an existing schedule.
        :param queue: Name of the queue which the scheduled messages will be enqueued.
        :param flow_control: Settings for controlling the number of active requests, \
            as well as the rate of requests with the same flow control key.
        """
        return self.create(
            destination=destination,
            cron=cron,
            body=json.dumps(body),
            content_type="application/json",
            method=method,
            headers=headers,
            callback_headers=callback_headers,
            failure_callback_headers=failure_callback_headers,
            retries=retries,
            callback=callback,
            failure_callback=failure_callback,
            delay=delay,
            timeout=timeout,
            schedule_id=schedule_id,
            queue=queue,
            flow_control=flow_control,
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
