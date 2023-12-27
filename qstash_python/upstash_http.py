import requests
from RetryConfig import RetryConfig
from typing import Union
from qstash_types import UpstashRequest

class HttpClient:
  def __init__(self, token: str, retry: Union[RetryConfig, bool], base_url: str):
    """
    Initializes the HttpClient.

    :param token: The authorization token from the upstash console.
    :param retry: The retry configuration object, which defines the retry behavior. False to disable retry.
    :param base_url: The base URL for the HTTP client. Trailing slashes are automatically removed.
    """
    self.base_url = base_url.rstrip("/")
    self.token = f"Bearer {token}"
    self.retry = RetryConfig(0, lambda _: 0) if retry == False else retry

  def request(self, req: UpstashRequest) -> dict:
    """
    Sends an HTTP request.

    :param req: The request object.
    :return: The response object.
    """
    init_headers = {
      "Authorization": self.token,
      "Content-Type": "application/json", # TODO: Make this configurable, but set for publishJSON
    }

    headers = {**init_headers, **req.get("headers")}
    
    # TODO: Does this need to be async?
    res = requests.post(
      url=f"{self.base_url}/{'/'.join(req.get('path'))}",
      headers=headers, 
      json=req.get("body")
    )

    print(res)
    
     