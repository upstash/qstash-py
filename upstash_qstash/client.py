from typing import Optional, Union
from upstash_qstash.upstash_http import HttpClient
from upstash_qstash.qstash_types import RetryConfig
from upstash_qstash.publish import (
    Publish,
    PublishRequest,
    PublishToUrlResponse,
    PublishToTopicResponse,
)
from upstash_qstash.messages import Messages
from upstash_qstash.topics import Topics
from upstash_qstash.dlq import DLQ
from upstash_qstash.events import Events, EventsRequest, GetEventsResponse
from upstash_qstash.schedules import Schedules

DEFAULT_BASE_URL = "https://qstash.upstash.io"


class Client:
    def __init__(
        self,
        token: str,
        retry: Optional[Union[bool, RetryConfig]] = None,
        base_url: Optional[str] = DEFAULT_BASE_URL,
    ):
        self.http = HttpClient(token, retry, base_url)

    def publish(
        self, req: PublishRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Publish a message to QStash.

        If publishing to a URL (req contains 'url'), this method returns a PublishToUrlResponse:
        - PublishToUrlResponse: Contains 'messageId' indicating the unique ID of the message and
        an optional 'deduplicated' boolean indicating if the message is a duplicate.

        If publishing to a topic (req contains 'topic'), it returns a PublishToTopicResponse:
        - PublishToTopicResponse: Contains a list of PublishToTopicSingleResponse objects, each of which
        contains 'messageId' indicating the unique ID of the message, 'url' indicating the URL to which
        the message was published, and an optional 'deduplicated' boolean indicating if the message is a duplicate.

        :param req: An instance of PublishRequest containing the request details.
        :return: Response details including the message_id, url (if publishing to a topic),
                and possibly a deduplicated boolean. The exact return type depends on the publish target.
        :raises ValueError: If neither 'url' nor 'topic' is provided, or both are provided.
        """
        return Publish.publish(self.http, req)

    def publish_json(
        self, req: PublishRequest
    ) -> Union[PublishToUrlResponse, PublishToTopicResponse]:
        """
        Publish a message to QStash, automatically serializing the body as JSON.

        :param req: An instance of PublishRequest containing the request details.
        :return: An instance of PublishResponse containing the response details.
        """
        return Publish.publish_json(self.http, req)

    def messages(self):
        """
        Access the messages API.

        Read or cancel messages.
        """
        return Messages(self.http)

    def topics(self):
        """
        Access the topics API.

        Create, read, update, or delete topics.
        """
        return Topics(self.http)

    def dlq(self):
        """
        Access the dlq API.

        Read or remove messages from the DLQ.s
        """
        return DLQ(self.http)

    def schedules(self):
        """
        Access the schedules API.

        Create, read, update, or delete schedules.
        """
        return Schedules(self.http)

    def events(self, req: Optional[EventsRequest] = None) -> GetEventsResponse:
        """
        Retrieve your logs.

        The logs endpoint is paginated and returns only 100 logs at a time.
        If you want to receive more logs, you can use the cursor to paginate.
        The cursor is a unix timestamp with millisecond precision

        :param req: An instance of EventsRequest containing the cursor
        :return: The events response object.

        Example:
        --------
        Initialize the cursor to the current timestamp in milliseconds:
        >>> cursor = int(time.time() * 1000)
        >>> logs = []
        >>> while cursor > 0:
        >>>     res = get({"cursor"=cursor})
        >>>     logs.extend(res['logs'])
        >>>     cursor = res.get('cursor', 0)
        """
        return Events.get(self.http, req)
