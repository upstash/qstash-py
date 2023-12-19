from qstash_python.RetryConfig import RetryConfig

class HttpClient:
  def __init__(self, token: str, retry: RetryConfig, base_url: str):
    """
    Initializes the HttpClient.

    :param token: The authorization token from the upstash console.
    :param retry: The retry configuration object, which defines the retry behavior.
    :param base_url: The base URL for the HTTP client. Trailing slashes are automatically removed.
    """
    self.base_url = base_url.rstrip("/")
    self.token = f"Bearer {token}"
    self.retry = retry
