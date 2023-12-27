from typing import Optional, Dict, Any, List, TypedDict
from enum import Enum

class Method(Enum):
  GET = "GET"
  POST = "POST"
  PUT = "PUT"
  DELETE = "DELETE"
  PATCH = "PATCH"

PublishRequest = TypedDict('PublishRequest', {
  'url': str,
  'body': Any,
  'headers': Optional[Dict[str, str]], # TODO: Figure out the type for this
  'delay': Optional[int],
  'notBefore': Optional[int],
  'deduplicationId': Optional[str],
  'contentBasedDeduplication': Optional[bool],
  'retries': Optional[int],
  'callback': Optional[str],
  'failureCallback': Optional[str],
  'method': Optional[Method],
  'topic': Optional[str],
})

UpstashRequest = TypedDict('UpstashRequest', {
  'path': List[str],
  'body': Optional[Any],
  'headers': Optional[Dict[str, str]],
  'keepalive': Optional[bool],
  'method': Optional[Method],
  'query': Optional[Dict[str, str]],
  'parseResponseAsJson': Optional[bool],
})
