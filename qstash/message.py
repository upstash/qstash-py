import dataclasses
import json
from typing import (
    Union,
    Optional,
    Literal,
    Dict,
    Any,
    List,
    TypedDict,
)

from qstash.chat import LlmProvider, UPSTASH_LLM_PROVIDER
from qstash.errors import QStashError
from qstash.http import HttpClient, HttpMethod


class LlmApi(TypedDict):
    name: Literal["llm"]
    """The name of the API type."""

    provider: LlmProvider
    """
    The LLM provider for the API.
    """


ApiT = LlmApi  # In the future, this can be union of different API types


@dataclasses.dataclass
class PublishResponse:
    message_id: str
    """The unique id of the message."""

    deduplicated: bool
    """Whether the message is a duplicate and was not sent to the destination."""


@dataclasses.dataclass
class PublishUrlGroupResponse:
    message_id: str
    """The unique id of the message."""

    url: str
    """The url where the message was sent to."""

    deduplicated: bool
    """Whether the message is a duplicate and was not sent to the destination."""


@dataclasses.dataclass
class EnqueueResponse:
    message_id: str
    """The unique id of the message."""

    deduplicated: bool
    """Whether the message is a duplicate and was not sent to the destination."""


@dataclasses.dataclass
class EnqueueUrlGroupResponse:
    message_id: str
    """The unique id of the message."""

    url: str
    """The url where the message was sent to."""

    deduplicated: bool
    """Whether the message is a duplicate and was not sent to the destination."""


@dataclasses.dataclass
class BatchResponse:
    message_id: str
    """The unique id of the message."""

    deduplicated: bool
    """Whether the message is a duplicate and was not sent to the destination."""


@dataclasses.dataclass
class BatchUrlGroupResponse:
    message_id: str
    """The unique id of the message."""

    url: str
    """The url where the message was sent to."""

    deduplicated: bool
    """Whether the message is a duplicate and was not sent to the destination."""


class BatchRequest(TypedDict, total=False):
    queue: str
    """Name of the queue that message will be enqueued on."""

    url: str
    """Url to send the message to."""

    url_group: str
    """Url group to send the message to."""

    api: ApiT
    """Api to send the message to."""

    body: Union[str, bytes]
    """The raw request message body passed to the endpoints as is."""

    content_type: str
    """MIME type of the message."""

    method: HttpMethod
    """The HTTP method to use when sending a webhook to your API."""

    headers: Dict[str, str]
    """Headers to forward along with the message."""

    retries: int
    """
    How often should this message be retried in case the destination 
    API is not available.
    """

    callback: str
    """A callback url that will be called after each attempt."""

    failure_callback: str
    """
    A failure callback url that will be called when a delivery is failed, 
    that is when all the defined retries are exhausted.
    """

    delay: Union[str, int]
    """
    Delay the message delivery. 
    
    The format for the delay string is a
    number followed by duration abbreviation, like `10s`. Available durations
    are `s` (seconds), `m` (minutes), `h` (hours), and `d` (days). As convenience,
    it is also possible to specify the delay as an integer, which will be
    interpreted as delay in seconds.
    """

    not_before: int
    """
    Delay the message until a certain time in the future. 
    
    The format is a unix timestamp in seconds, based on the UTC timezone.
    """

    deduplication_id: str
    """Id to use while deduplicating messages."""

    content_based_deduplication: bool
    """Automatically deduplicate messages based on their content."""

    timeout: Union[str, int]
    """
    The HTTP timeout value to use while calling the destination URL.
    When a timeout is specified, it will be used instead of the maximum timeout
    value permitted by the QStash plan. It is useful in scenarios, where a message
    should be delivered with a shorter timeout.
    
    The format for the timeout string is a number followed by duration abbreviation, 
    like `10s`. Available durations are `s` (seconds), `m` (minutes), `h` (hours), 
    and `d` (days). As convenience, it is also possible to specify the timeout as 
    an integer, which will be interpreted as timeout in seconds.
    """


class BatchJsonRequest(TypedDict, total=False):
    queue: str
    """Name of the queue that message will be enqueued on."""

    url: str
    """Url to send the message to."""

    url_group: str
    """Url group to send the message to."""

    api: ApiT
    """Api to send the message to."""

    body: Any
    """
    The request body passed to the endpoints after being serialized to 
    JSON string.
    """

    method: HttpMethod
    """The HTTP method to use when sending a webhook to your API."""

    headers: Dict[str, str]
    """Headers to forward along with the message."""

    retries: int
    """
    How often should this message be retried in case the destination 
    API is not available.
    """

    callback: str
    """A callback url that will be called after each attempt."""

    failure_callback: str
    """
    A failure callback url that will be called when a delivery is failed, 
    that is when all the defined retries are exhausted.
    """

    delay: Union[str, int]
    """
    Delay the message delivery. 
    
    The format for the delay string is a
    number followed by duration abbreviation, like `10s`. Available durations
    are `s` (seconds), `m` (minutes), `h` (hours), and `d` (days). As convenience,
    it is also possible to specify the delay as an integer, which will be
    interpreted as delay in seconds.
    """

    not_before: int
    """
    Delay the message until a certain time in the future. 
    
    The format is a unix timestamp in seconds, based on the UTC timezone.
    """

    deduplication_id: str
    """Id to use while deduplicating messages."""

    content_based_deduplication: bool
    """Automatically deduplicate messages based on their content."""

    timeout: Union[str, int]
    """
    The HTTP timeout value to use while calling the destination URL.
    When a timeout is specified, it will be used instead of the maximum timeout
    value permitted by the QStash plan. It is useful in scenarios, where a message
    should be delivered with a shorter timeout.

    The format for the timeout string is a number followed by duration abbreviation, 
    like `10s`. Available durations are `s` (seconds), `m` (minutes), `h` (hours), 
    and `d` (days). As convenience, it is also possible to specify the timeout as 
    an integer, which will be interpreted as timeout in seconds.
    """

    provider: LlmProvider
    """
    LLM provider to use. 
    
    When specified, destination and headers will be
    set according to the LLM provider.
    """


@dataclasses.dataclass
class Message:
    message_id: str
    """The unique identifier of the message."""

    url: str
    """The url to which the message should be delivered."""

    url_group: Optional[str]
    """The url group name if this message was sent to a url group."""

    endpoint: Optional[str]
    """
    The endpoint name of the message if the endpoint is given a 
    name within the url group.
    """

    api: Optional[str]
    """The api name if this message was sent to an api."""

    queue: Optional[str]
    """The queue name if this message was enqueued to a queue."""

    body: Optional[str]
    """
    The body of the message if it is composed of UTF-8 characters only, 
    `None` otherwise.
    """

    body_base64: Optional[str]
    """
    The base64 encoded body if the body contains non-UTF-8 characters, 
    `None` otherwise.
    """

    method: HttpMethod
    """The HTTP method to use for the message."""

    headers: Optional[Dict[str, List[str]]]
    """The HTTP headers sent the endpoint."""

    max_retries: int
    """The number of retries that should be attempted in case of delivery failure."""

    not_before: int
    """The unix timestamp in milliseconds before which the message should not be delivered."""

    created_at: int
    """The unix timestamp in milliseconds when the message was created."""

    callback: Optional[str]
    """The url which is called each time the message is attempted to be delivered."""

    failure_callback: Optional[str]
    """The url which is called after the message is failed."""

    schedule_id: Optional[str]
    """The scheduleId of the message if the message is triggered by a schedule."""

    caller_ip: Optional[str]
    """IP address of the publisher of this message."""


def get_destination(
    *,
    url: Optional[str],
    url_group: Optional[str],
    api: Optional[ApiT],
    headers: Dict[str, str],
) -> str:
    destination = None
    count = 0
    if url is not None:
        destination = url
        count += 1

    if url_group is not None:
        destination = url_group
        count += 1

    if api is not None:
        provider = api["provider"]
        if provider.name == UPSTASH_LLM_PROVIDER.name:
            destination = "api/llm"
        else:
            destination = provider.base_url + "/v1/chat/completions"
            headers["Authorization"] = f"Bearer {provider.token}"

        count += 1

    if count != 1:
        raise QStashError(
            "Only and only one of 'url', 'url_group', or 'api' must be provided."
        )

    # Can't be None at this point
    return destination  # type:ignore[return-value]


def prepare_headers(
    *,
    content_type: Optional[str],
    method: Optional[HttpMethod],
    headers: Optional[Dict[str, str]],
    retries: Optional[int],
    callback: Optional[str],
    failure_callback: Optional[str],
    delay: Optional[Union[str, int]],
    not_before: Optional[int],
    deduplication_id: Optional[str],
    content_based_deduplication: Optional[bool],
    timeout: Optional[Union[str, int]],
) -> Dict[str, str]:
    h = {}

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

    if not_before is not None:
        h["Upstash-Not-Before"] = str(not_before)

    if deduplication_id is not None:
        h["Upstash-Deduplication-Id"] = deduplication_id

    if content_based_deduplication is not None:
        h["Upstash-Content-Based-Deduplication"] = str(content_based_deduplication)

    if timeout is not None:
        if isinstance(timeout, int):
            h["Upstash-Timeout"] = f"{timeout}s"
        else:
            h["Upstash-Timeout"] = timeout

    return h


def parse_publish_response(
    response: Union[List[Dict[str, Any]], Dict[str, Any]],
) -> Union[PublishResponse, List[PublishUrlGroupResponse]]:
    if isinstance(response, list):
        result = []
        for ug_resp in response:
            result.append(
                PublishUrlGroupResponse(
                    message_id=ug_resp["messageId"],
                    url=ug_resp["url"],
                    deduplicated=ug_resp.get("deduplicated", False),
                )
            )

        return result

    return PublishResponse(
        message_id=response["messageId"],
        deduplicated=response.get("deduplicated", False),
    )


def parse_enqueue_response(
    response: Union[List[Dict[str, Any]], Dict[str, Any]],
) -> Union[EnqueueResponse, List[EnqueueUrlGroupResponse]]:
    if isinstance(response, list):
        result = []
        for ug_resp in response:
            result.append(
                EnqueueUrlGroupResponse(
                    message_id=ug_resp["messageId"],
                    url=ug_resp["url"],
                    deduplicated=ug_resp.get("deduplicated", False),
                )
            )

        return result

    return EnqueueResponse(
        message_id=response["messageId"],
        deduplicated=response.get("deduplicated", False),
    )


def prepare_batch_message_body(messages: List[BatchRequest]) -> str:
    batch_messages = []

    for msg in messages:
        user_headers = msg.get("headers") or {}
        destination = get_destination(
            url=msg.get("url"),
            url_group=msg.get("url_group"),
            api=msg.get("api"),
            headers=user_headers,
        )

        headers = prepare_headers(
            content_type=msg.get("content_type"),
            method=msg.get("method"),
            headers=user_headers,
            retries=msg.get("retries"),
            callback=msg.get("callback"),
            failure_callback=msg.get("failure_callback"),
            delay=msg.get("delay"),
            not_before=msg.get("not_before"),
            deduplication_id=msg.get("deduplication_id"),
            content_based_deduplication=msg.get("content_based_deduplication"),
            timeout=msg.get("timeout"),
        )

        batch_messages.append(
            {
                "destination": destination,
                "headers": headers,
                "body": msg.get("body"),
                "queue": msg.get("queue"),
            }
        )

    return json.dumps(batch_messages)


def parse_batch_response(
    response: List[Union[List[Dict[str, Any]], Dict[str, Any]]],
) -> List[Union[BatchResponse, List[BatchUrlGroupResponse]]]:
    result: List[Union[BatchResponse, List[BatchUrlGroupResponse]]] = []

    for resp in response:
        if isinstance(resp, list):
            ug_result = []
            for ug_resp in resp:
                ug_result.append(
                    BatchUrlGroupResponse(
                        message_id=ug_resp["messageId"],
                        url=ug_resp["url"],
                        deduplicated=ug_resp.get("deduplicated", False),
                    )
                )

            result.append(ug_result)
        else:
            result.append(
                BatchResponse(
                    message_id=resp["messageId"],
                    deduplicated=resp.get("deduplicated", False),
                )
            )

    return result


def convert_to_batch_messages(
    messages: List[BatchJsonRequest],
) -> List[BatchRequest]:
    batch_messages = []

    for msg in messages:
        batch_msg: BatchRequest = {}
        if "queue" in msg:
            batch_msg["queue"] = msg["queue"]

        if "url" in msg:
            batch_msg["url"] = msg["url"]

        if "url_group" in msg:
            batch_msg["url_group"] = msg["url_group"]

        if "api" in msg:
            batch_msg["api"] = msg["api"]

        batch_msg["body"] = json.dumps(msg.get("body"))
        batch_msg["content_type"] = "application/json"

        if "method" in msg:
            batch_msg["method"] = msg["method"]

        if "headers" in msg:
            batch_msg["headers"] = msg["headers"]

        if "retries" in msg:
            batch_msg["retries"] = msg["retries"]

        if "callback" in msg:
            batch_msg["callback"] = msg["callback"]

        if "failure_callback" in msg:
            batch_msg["failure_callback"] = msg["failure_callback"]

        if "delay" in msg:
            batch_msg["delay"] = msg["delay"]

        if "not_before" in msg:
            batch_msg["not_before"] = msg["not_before"]

        if "deduplication_id" in msg:
            batch_msg["deduplication_id"] = msg["deduplication_id"]

        if "content_based_deduplication" in msg:
            batch_msg["content_based_deduplication"] = msg[
                "content_based_deduplication"
            ]

        if "timeout" in msg:
            batch_msg["timeout"] = msg["timeout"]

        batch_messages.append(batch_msg)

    return batch_messages


def parse_message_response(response: Dict[str, Any]) -> Message:
    return Message(
        message_id=response["messageId"],
        url=response["url"],
        url_group=response.get("topicName"),
        endpoint=response.get("endpointName"),
        api=response.get("api"),
        queue=response.get("queueName"),
        body=response.get("body"),
        body_base64=response.get("bodyBase64"),
        method=response["method"],
        headers=response.get("header"),
        max_retries=response["maxRetries"],
        not_before=response["notBefore"],
        created_at=response["createdAt"],
        callback=response.get("callback"),
        failure_callback=response.get("failureCallback"),
        schedule_id=response.get("scheduleId"),
        caller_ip=response.get("callerIP"),
    )


class MessageApi:
    def __init__(self, http: HttpClient):
        self._http = http

    def publish(
        self,
        *,
        url: Optional[str] = None,
        url_group: Optional[str] = None,
        api: Optional[ApiT] = None,
        body: Optional[Union[str, bytes]] = None,
        content_type: Optional[str] = None,
        method: Optional[HttpMethod] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        callback: Optional[str] = None,
        failure_callback: Optional[str] = None,
        delay: Optional[Union[str, int]] = None,
        not_before: Optional[int] = None,
        deduplication_id: Optional[str] = None,
        content_based_deduplication: Optional[bool] = None,
        timeout: Optional[Union[str, int]] = None,
    ) -> Union[PublishResponse, List[PublishUrlGroupResponse]]:
        """
        Publishes a message to QStash.

        If the destination is a `url` or an `api`, `PublishResponse`
        is returned.

        If the destination is a `url_group`, then a list of
        `PublishUrlGroupResponse`s are returned, one for each url
        in the url group.

        :param url: Url to send the message to.
        :param url_group: Url group to send the message to.
        :param api: Api to send the message to.
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
        :param not_before: Delay the message until a certain time in the future.
            The format is a unix timestamp in seconds, based on the UTC timezone.
        :param deduplication_id: Id to use while deduplicating messages.
        :param content_based_deduplication: Automatically deduplicate messages based on
            their content.
        :param timeout: The HTTP timeout value to use while calling the destination URL.
            When a timeout is specified, it will be used instead of the maximum timeout
            value permitted by the QStash plan. It is useful in scenarios, where a message
            should be delivered with a shorter timeout.
        """
        headers = headers or {}
        destination = get_destination(
            url=url,
            url_group=url_group,
            api=api,
            headers=headers,
        )

        req_headers = prepare_headers(
            content_type=content_type,
            method=method,
            headers=headers,
            retries=retries,
            callback=callback,
            failure_callback=failure_callback,
            delay=delay,
            not_before=not_before,
            deduplication_id=deduplication_id,
            content_based_deduplication=content_based_deduplication,
            timeout=timeout,
        )

        response = self._http.request(
            path=f"/v2/publish/{destination}",
            method="POST",
            headers=req_headers,
            body=body,
        )

        return parse_publish_response(response)

    def publish_json(
        self,
        *,
        url: Optional[str] = None,
        url_group: Optional[str] = None,
        api: Optional[ApiT] = None,
        body: Optional[Any] = None,
        method: Optional[HttpMethod] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        callback: Optional[str] = None,
        failure_callback: Optional[str] = None,
        delay: Optional[Union[str, int]] = None,
        not_before: Optional[int] = None,
        deduplication_id: Optional[str] = None,
        content_based_deduplication: Optional[bool] = None,
        timeout: Optional[Union[str, int]] = None,
    ) -> Union[PublishResponse, List[PublishUrlGroupResponse]]:
        """
        Publish a message to QStash, automatically serializing the
        body as JSON string, and setting content type to `application/json`.

        If the destination is a `url` or an `api`, `PublishResponse`
        is returned.

        If the destination is a `url_group`, then a list of
        `PublishUrlGroupResponse`s are returned, one for each url
        in the url group.

        :param url: Url to send the message to.
        :param url_group: Url group to send the message to.
        :param api: Api to send the message to.
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
        :param not_before: Delay the message until a certain time in the future.
            The format is a unix timestamp in seconds, based on the UTC timezone.
        :param deduplication_id: Id to use while deduplicating messages.
        :param content_based_deduplication: Automatically deduplicate messages based on
            their content.
        :param timeout: The HTTP timeout value to use while calling the destination URL.
            When a timeout is specified, it will be used instead of the maximum timeout
            value permitted by the QStash plan. It is useful in scenarios, where a message
            should be delivered with a shorter timeout.
        """
        return self.publish(
            url=url,
            url_group=url_group,
            api=api,
            body=json.dumps(body),
            content_type="application/json",
            method=method,
            headers=headers,
            retries=retries,
            callback=callback,
            failure_callback=failure_callback,
            delay=delay,
            not_before=not_before,
            deduplication_id=deduplication_id,
            content_based_deduplication=content_based_deduplication,
            timeout=timeout,
        )

    def enqueue(
        self,
        *,
        queue: str,
        url: Optional[str] = None,
        url_group: Optional[str] = None,
        api: Optional[ApiT] = None,
        body: Optional[Union[str, bytes]] = None,
        content_type: Optional[str] = None,
        method: Optional[HttpMethod] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        callback: Optional[str] = None,
        failure_callback: Optional[str] = None,
        delay: Optional[Union[str, int]] = None,
        not_before: Optional[int] = None,
        deduplication_id: Optional[str] = None,
        content_based_deduplication: Optional[bool] = None,
        timeout: Optional[Union[str, int]] = None,
    ) -> Union[EnqueueResponse, List[EnqueueUrlGroupResponse]]:
        """
        Enqueues a message, after creating the queue if it does
        not exist.

        If the destination is a `url` or an `api`, `EnqueueResponse`
        is returned.

        If the destination is a `url_group`, then a list of
        `EnqueueUrlGroupResponse`s are returned, one for each url
        in the url group.

        :param queue: The name of the queue.
        :param url: Url to send the message to.
        :param url_group: Url group to send the message to.
        :param api: Api to send the message to.
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
        :param not_before: Delay the message until a certain time in the future.
            The format is a unix timestamp in seconds, based on the UTC timezone.
        :param deduplication_id: Id to use while deduplicating messages.
        :param content_based_deduplication: Automatically deduplicate messages based on
            their content.
        :param timeout: The HTTP timeout value to use while calling the destination URL.
            When a timeout is specified, it will be used instead of the maximum timeout
            value permitted by the QStash plan. It is useful in scenarios, where a message
            should be delivered with a shorter timeout.
        """
        headers = headers or {}
        destination = get_destination(
            url=url,
            url_group=url_group,
            api=api,
            headers=headers,
        )

        req_headers = prepare_headers(
            content_type=content_type,
            method=method,
            headers=headers,
            retries=retries,
            callback=callback,
            failure_callback=failure_callback,
            delay=delay,
            not_before=not_before,
            deduplication_id=deduplication_id,
            content_based_deduplication=content_based_deduplication,
            timeout=timeout,
        )

        response = self._http.request(
            path=f"/v2/enqueue/{queue}/{destination}",
            method="POST",
            headers=req_headers,
            body=body,
        )

        return parse_enqueue_response(response)

    def enqueue_json(
        self,
        *,
        queue: str,
        url: Optional[str] = None,
        url_group: Optional[str] = None,
        api: Optional[ApiT] = None,
        body: Optional[Any] = None,
        method: Optional[HttpMethod] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        callback: Optional[str] = None,
        failure_callback: Optional[str] = None,
        delay: Optional[Union[str, int]] = None,
        not_before: Optional[int] = None,
        deduplication_id: Optional[str] = None,
        content_based_deduplication: Optional[bool] = None,
        timeout: Optional[Union[str, int]] = None,
    ) -> Union[EnqueueResponse, List[EnqueueUrlGroupResponse]]:
        """
        Enqueues a message, after creating the queue if it does
        not exist. It automatically serializes the body as JSON string,
        and setting content type to `application/json`.

        If the destination is a `url` or an `api`, `EnqueueResponse`
        is returned.

        If the destination is a `url_group`, then a list of
        `EnqueueUrlGroupResponse`s are returned, one for each url
        in the url group.

        :param queue: The name of the queue.
        :param url: Url to send the message to.
        :param url_group: Url group to send the message to.
        :param api: Api to send the message to.
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
        :param not_before: Delay the message until a certain time in the future.
            The format is a unix timestamp in seconds, based on the UTC timezone.
        :param deduplication_id: Id to use while deduplicating messages.
        :param content_based_deduplication: Automatically deduplicate messages based on
            their content.
        :param timeout: The HTTP timeout value to use while calling the destination URL.
            When a timeout is specified, it will be used instead of the maximum timeout
            value permitted by the QStash plan. It is useful in scenarios, where a message
            should be delivered with a shorter timeout.
        """
        return self.enqueue(
            queue=queue,
            url=url,
            url_group=url_group,
            api=api,
            body=json.dumps(body),
            content_type="application/json",
            method=method,
            headers=headers,
            retries=retries,
            callback=callback,
            failure_callback=failure_callback,
            delay=delay,
            not_before=not_before,
            deduplication_id=deduplication_id,
            content_based_deduplication=content_based_deduplication,
            timeout=timeout,
        )

    def batch(
        self, messages: List[BatchRequest]
    ) -> List[Union[BatchResponse, List[BatchUrlGroupResponse]]]:
        """
        Publishes or enqueues multiple messages in a single request.

        Returns a list of publish or enqueue responses, one for each
        message in the batch.

        If the message in the batch is sent to a url or an API,
        the corresponding item in the response is `BatchResponse`.

        If the message in the batch is sent to a url group,
        the corresponding item in the response is list of
        `BatchUrlGroupResponse`s, one for each url in the url group.
        """
        body = prepare_batch_message_body(messages)

        response = self._http.request(
            path="/v2/batch",
            body=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        return parse_batch_response(response)

    def batch_json(
        self, messages: List[BatchJsonRequest]
    ) -> List[Union[BatchResponse, List[BatchUrlGroupResponse]]]:
        """
        Publishes or enqueues multiple messages in a single request,
        automatically serializing the message bodies as JSON strings,
        and setting content type to `application/json`.

        Returns a list of publish or enqueue responses, one for each
        message in the batch.

        If the message in the batch is sent to a url or an API,
        the corresponding item in the response is `BatchResponse`.

        If the message in the batch is sent to a url group,
        the corresponding item in the response is list of
        `BatchUrlGroupResponse`s, one for each url in the url group.
        """
        batch_messages = convert_to_batch_messages(messages)
        return self.batch(batch_messages)

    def get(self, message_id: str) -> Message:
        """
        Gets the message by its id.
        """
        response = self._http.request(
            path=f"/v2/messages/{message_id}",
            method="GET",
        )

        return parse_message_response(response)

    def cancel(self, message_id: str) -> None:
        """
        Cancels delivery of an existing message.

        Cancelling a message will remove it from QStash and stop it from being
        delivered in the future. If a message is in flight to your API,
        it might be too late to cancel.
        """
        self._http.request(
            path=f"/v2/messages/{message_id}",
            method="DELETE",
            parse_response=False,
        )

    def cancel_many(self, message_ids: List[str]) -> int:
        """
        Cancels delivery of existing messages.

        Cancelling a message will remove it from QStash and stop it from being
        delivered in the future. If a message is in flight to your API,
        it might be too late to cancel.

        Returns how many of the messages are cancelled.
        """
        body = json.dumps({"messageIds": message_ids})

        response = self._http.request(
            path="/v2/messages",
            method="DELETE",
            headers={"Content-Type": "application/json"},
            body=body,
        )

        return response["cancelled"]

    def cancel_all(self):
        """
        Cancels delivery of all the existing messages.

        Cancelling a message will remove it from QStash and stop it from being
        delivered in the future. If a message is in flight to your API,
        it might be too late to cancel.

        Returns how many messages are cancelled.
        """
        response = self._http.request(
            path="/v2/messages",
            method="DELETE",
        )

        return response["cancelled"]
