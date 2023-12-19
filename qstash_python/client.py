from typing import Optional
from qstash_python.http import HttpClient
from qstash_python.RetryConfig import RetryConfig
from qstash_python.qstash_types import PublishRequest

DEFAULT_BASE_URL = "https://qstash.upstash.io"

class PublishResponse:
  def __init__(self, ):
    pass
  
class Client:
  def __init__(self, token: str, retry: Optional[RetryConfig] = RetryConfig(), base_url: Optional[str] = DEFAULT_BASE_URL):
    self.http = HttpClient(token, retry, base_url)
    
  async def publish(self, req: PublishRequest) -> PublishResponse:
    """
    Publish a message to QStash.

    :param req: An instance of PublishRequest containing the request details.
    :return: An instance of PublishResponse containing the response details.
    """
    headers = Headers(prefix_headers(req.headers)) 

    headers.set("Upstash-Method", (req.attr_exists("method") and req.method) or "POST")

    if req.attr_exists("delay"):
      headers.set("Upstash-Delay", f"{req.delay}s")

    if req.attr_exists("not_before"):
      headers.set("Upstash-Not-Before", str(req.not_before))

    if req.attr_exists("deduplication_id"):
      headers.set("Upstash-Deduplication-Id", req.deduplication_id)

    if req.attr_exists("content_based_deduplication"):
      headers.set("Upstash-Content-Based-Deduplication", "true")

    if req.attr_exists("retries"):
      headers.set("Upstash-Retries", str(req.retries))

    if req.attr_exists("callback"):
      headers.set("Upstash-Callback", req.callback)

    if req.attr_exists("failure_callback"):
      headers.set("Upstash-Failure-Callback", req.failure_callback)

    res = await self.http.request({
      "path": ["v2", "publish", (req.attr_exists("url") and req.url) or req.topic],
      "body": req.body,
      "headers": headers,
      "method": "POST"
    })

    return res