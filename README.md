# Upstash Python QStash SDK

**QStash** is an HTTP based messaging and scheduling solution for serverless and edge runtimes.

_Note: This SDK is not available on PyPi yet. You can clone this repository and use it. Asyncio is not ready yet._

[QStash Documentation](https://upstash.com/docs/qstash)

### Usage

You can get your QStash token from the [Upstash Console](https://console.upstash.com/qstash).

#### Publish a JSON message
```python
from upstash_qstash import Client

client = Client("QSTASH_TOKEN")
client.publish_json(
  {
    "url": "https://my-api...",
    "body": {
      "hello": "world"
    },
    "headers": {
        "test-header": "test-value",
    },
  }
)
```

#### [Create a scheduled message](https://upstash.com/docs/qstash/features/schedules)
```python
from upstash_qstash import Client

client = Client("QSTASH_TOKEN")
schedules = client.schedules()
schedules.create({
  "destination": "https://my-api...",
  "cron": "*/5 * * * *",
})
```

#### [Receiving messages](https://upstash.com/docs/qstash/howto/receiving)
```python
from upstash_qstash import Client

# Keys available from the QStash console
receiver = Receiver(
  {
    "current_signing_key": "CURRENT_SIGNING_KEY",
    "next_signing_key": "NEXT_SIGNING_KEY",
  }
)

verified = receiver.verify(
  {
    "signature": req.headers["Upstash-Signature"],
    "body": req.body,
    "url": "https://my-api...", # Optional
  }
)
```

#### Additional configuration
```python
from upstash_qstash import Client

client = Client("QSTASH_TOKEN")
  # Create a client with a custom retry configuration. This is 
  # for sending messages to QStash, not for sending messages to
  # your endpoints.
  # The default configuration is:
  # {
  #   "attempts": 6,
  #   "backoff": lambda retry_count: math.exp(retry_count) * 50,
  # }
  client = Client("QSTASH_TOKEN", {
    "attempts": 2,
    "backoff": lambda retry_count: (2 ** retry_count) * 20,
  })
  
  # Create Topic
  topics = client.topics()
  topics.add_endpoints("my-topic", [
    {
      "name": "endpoint1",
      "url": "https://example.com"
    },
    {
      "name": "endpoint2",
      "url": "https://somewhere-else.com"
    }
  ])

  # Publish to Topic
  client.publish_json({
    "topic": "my-topic",
    "body": {
      "key": "value"
    },
    # Retry sending message to API 3 times
    # https://upstash.com/docs/qstash/features/retry
    "retries": 3,
    # Schedule message to be sent 4 seconds from now
    "delay": 4, 
    # When message is sent, send a request to this URL
    # https://upstash.com/docs/qstash/features/callbacks
    "callback": "https://my-api.com/callback",
    # When message fails to send, send a request to this URL
    "failure_callback": "https://my-api.com/failure_callback",
    # Headers to forward to the endpoint
    "headers": {
      "test-header": "test-value",
    },
    # Enable content-based deduplication
    # https://upstash.com/docs/qstash/features/deduplication#content-based-deduplication
    "content_based_deduplication": True,
  })
```

Additional methods are available for managing topics, schedules, and messages. See the examples folder for more. (TODO)

### Development
1. Clone the repository
2. Install [Poetry](https://python-poetry.org/docs/#installation)
3. Install dependencies with `poetry install`
4. Run tests with `poetry run pytest`
