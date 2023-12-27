from typing import Optional, Dict, Any, List, TypedDict

PublishRequest = TypedDict('PublishRequest', {
  'url': str,
  'body': Any,
  'headers': Optional[Dict[str, str]],
  'delay': Optional[int],
  'notBefore': Optional[int],
  'deduplicationId': Optional[str],
  'contentBasedDeduplication': Optional[bool],
  'retries': Optional[int],
  'callback': Optional[str],
  'failureCallback': Optional[str],
  'method': Optional[str],
  'topic': Optional[str],
})

UpstashRequest = TypedDict('UpstashRequest', {
  'path': List[str],
  'body': Optional[Any],
  'headers': Optional[Dict[str, str]],
  'keepalive': Optional[bool],
  'method': Optional[str],
  'query': Optional[Dict[str, str]],
  'parseResponseAsJson': Optional[bool],
})
