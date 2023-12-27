from typing import Optional
from upstash_http import HttpClient
from RetryConfig import RetryConfig
from qstash_types import PublishRequest

DEFAULT_BASE_URL = "https://qstash.upstash.io"

class PublishResponse:
  def __init__(self, ):
    pass
  
class Client:
  def __init__(self, token: str, retry: Optional[RetryConfig] = RetryConfig(), base_url: Optional[str] = DEFAULT_BASE_URL):
    self.http = HttpClient(token, retry, base_url)
    
  def publish(self, req: PublishRequest) -> PublishResponse:
    """
    Publish a message to QStash.

    :param req: An instance of PublishRequest containing the request details.
    :return: An instance of PublishResponse containing the response details.
    """
    # Request should have either url or topic, but not both
    if (req.get("url") is None and req.get("topic") is None) or (req.get("url") is not None and req.get("topic") is not None):
      raise ValueError("Either 'url' or 'topic' must be provided, but not both.")
    
    headers = Headers(prefix_headers(req.headers)) 

    headers.set("Upstash-Method", req.get("method") or "POST")

    if req.get("delay"):
      headers.set("Upstash-Delay", f"{req.get('delay')}s")

    if req.get("not_before"):
      headers.set("Upstash-Not-Before", str(req.get("not_before")))

    if req.get("deduplication_id"):
      headers.set("Upstash-Deduplication-Id", req.get("deduplication_id"))

    if req.get("content_based_deduplication"):
      headers.set("Upstash-Content-Based-Deduplication", "true")

    if req.get("retries"):
      headers.set("Upstash-Retries", str(req.get("retries")))

    if req.get("callback"):
      headers.set("Upstash-Callback", req.get("callback"))

    if req.get("failure_callback"):
      headers.set("Upstash-Failure-Callback", req.get("failure_callback"))

    res = self.http.request({
      "path": ["v2", "publish", req.get("url") or req.get("topic")],
      "body": req.get("body"),
      "headers": headers,
      "method": "POST"
    })

    return res