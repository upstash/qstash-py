import math


# TODO: Maybe convert to a TypedDict
def default_backoff(retry_count: int) -> int:
    """
    Default backoff function.

    :param retry_count: The current retry attempt count.
    :return: Time to wait in milliseconds before next retry, calculated as exp(retry_count) * 50.
    """
    return math.exp(retry_count) * 50


class RetryConfig:
    def __init__(self, retries: int = 5, backoff: callable = default_backoff):
        """
        Configuration for retry logic.

        :param retries: The number of retries to attempt before giving up. Defaults to 5.
        :param backoff: A backoff function that receives the current retry count and returns
                        a number in milliseconds to wait before retrying.
                        Defaults to math.exp(retry_count) * 50.
        """
        self.retries = retries
        self.backoff = backoff
