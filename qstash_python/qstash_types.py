from dataclasses import dataclass, field
from typing import Optional, Dict, Any

@dataclass
class PublishRequest:
  body: Optional[Any] = field(default=None, metadata={"doc": "The message to send. Can be of any type. Set the `Content-Type` header accordingly."})
  headers: Optional[Dict[str, str]] = field(default=None, metadata={"doc": "Optional headers to be sent with the message. Include `Content-Type` for clarity."})
  delay: Optional[int] = field(default=None, metadata={"doc": "Optional delay in seconds for message delivery."})
  notBefore: Optional[int] = field(default=None, metadata={"doc": "Optional Unix timestamp in seconds for absolute delay of message delivery."})
  deduplicationId: Optional[str] = field(default=None, metadata={"doc": "Optional unique id for message deduplication."})
  contentBasedDeduplication: Optional[bool] = field(default=False, metadata={"doc": "If True, the message content will be hashed for deduplication."})
  retries: Optional[int] = field(default=None, metadata={"doc": "Optional number of times for the delivery to be retried."})
  callback: Optional[str] = field(default=None, metadata={"doc": "Optional callback URL to forward the response of your destination server."})
  failureCallback: Optional[str] = field(default=None, metadata={"doc": "Optional failure callback URL for handling undelivered messages."})
  method: Optional[str] = field(default="POST", metadata={"doc": "HTTP method to use when sending a request. Defaults to `POST`."})
  url: Optional[str] = field(default=None, metadata={"doc": "The URL where the message should be sent."})
  topic: Optional[str] = field(default=None, metadata={"doc": "The topic where the message should be sent."})

  def __post_init__(self):
    if (self.url is None and self.topic is None) or (self.url is not None and self.topic is not None):
      raise ValueError("Either 'url' or 'topic' must be provided, but not both.")

  def attr_exists(self, attr: str):
    """
    Checks if the user has set the provided attribute.
    """
    return hasattr(self, attr) and getattr(self, attr) is not None
