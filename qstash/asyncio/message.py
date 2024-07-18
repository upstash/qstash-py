import json
from typing import Any, Dict, List, Optional, Union

from qstash.asyncio.http import AsyncHttpClient
from qstash.http import HttpMethod
from qstash.message import (
    ApiT,
    BatchJsonRequest,
    BatchRequest,
    BatchResponse,
    BatchUrlGroupResponse,
    EnqueueResponse,
    EnqueueUrlGroupResponse,
    Message,
    PublishResponse,
    PublishUrlGroupResponse,
    convert_to_batch_messages,
    get_destination,
    parse_batch_response,
    parse_enqueue_response,
    parse_message_response,
    parse_publish_response,
    prepare_batch_message_body,
    prepare_headers,
)


class AsyncMessageApi:
    def __init__(self, http: AsyncHttpClient):
        self._http = http

    async def publish(
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

        response = await self._http.request(
            path=f"/v2/publish/{destination}",
            method="POST",
            headers=req_headers,
            body=body,
        )

        return parse_publish_response(response)

    async def publish_json(
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
        return await self.publish(
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

    async def enqueue(
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

        response = await self._http.request(
            path=f"/v2/enqueue/{queue}/{destination}",
            method="POST",
            headers=req_headers,
            body=body,
        )

        return parse_enqueue_response(response)

    async def enqueue_json(
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
        return await self.enqueue(
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

    async def batch(
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

        response = await self._http.request(
            path="/v2/batch",
            body=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        return parse_batch_response(response)

    async def batch_json(
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
        return await self.batch(batch_messages)

    async def get(self, message_id: str) -> Message:
        """
        Gets the message by its id.
        """
        response = await self._http.request(
            path=f"/v2/messages/{message_id}",
            method="GET",
        )

        return parse_message_response(response)

    async def cancel(self, message_id: str) -> None:
        """
        Cancels delivery of an existing message.

        Cancelling a message will remove it from QStash and stop it from being
        delivered in the future. If a message is in flight to your API,
        it might be too late to cancel.
        """
        await self._http.request(
            path=f"/v2/messages/{message_id}",
            method="DELETE",
            parse_response=False,
        )

    async def cancel_many(self, message_ids: List[str]) -> int:
        """
        Cancels delivery of existing messages.

        Cancelling a message will remove it from QStash and stop it from being
        delivered in the future. If a message is in flight to your API,
        it might be too late to cancel.

        Returns how many of the messages are cancelled.
        """
        body = json.dumps({"messageIds": message_ids})

        response = await self._http.request(
            path="/v2/messages",
            method="DELETE",
            headers={"Content-Type": "application/json"},
            body=body,
        )

        return response["cancelled"]

    async def cancel_all(self):
        """
        Cancels delivery of all the existing messages.

        Cancelling a message will remove it from QStash and stop it from being
        delivered in the future. If a message is in flight to your API,
        it might be too late to cancel.

        Returns how many messages are cancelled.
        """
        response = await self._http.request(
            path="/v2/messages",
            method="DELETE",
        )

        return response["cancelled"]
