# Upstash Python QStash SDK

> [!NOTE]  
> **This project is in GA Stage.**
>
> The Upstash Professional Support fully covers this project. It receives regular updates, and bug fixes.
> The Upstash team is committed to maintaining and improving its functionality.

**QStash** is an HTTP based messaging and scheduling solution for serverless and edge runtimes.

[QStash Documentation](https://upstash.com/docs/qstash)

### Install

```shell
pip install qstash
```

### Usage

You can get your QStash token from the [Upstash Console](https://console.upstash.com/qstash).

#### Publish a JSON message

```python
from qstash import QStash

client = QStash("<QSTASH_TOKEN>")

res = client.message.publish_json(
    url="https://example.com",
    body={"hello": "world"},
    headers={
        "test-header": "test-value",
    },
)

print(res.message_id)
```

#### [Create a scheduled message](https://upstash.com/docs/qstash/features/schedules)

```python
from qstash import QStash

client = QStash("<QSTASH_TOKEN>")

schedule_id = client.schedule.create(
    destination="https://example.com",
    cron="*/5 * * * *",
)

print(schedule_id)
```

#### [Receiving messages](https://upstash.com/docs/qstash/howto/receiving)

```python
from qstash import Receiver

# Keys available from the QStash console
receiver = Receiver(
    current_signing_key="CURRENT_SIGNING_KEY",
    next_signing_key="NEXT_SIGNING_KEY",
)

# ... in your request handler

signature, body = req.headers["Upstash-Signature"], req.body

receiver.verify(
    body=body,
    signature=signature,
    url="https://example.com",  # Optional
)
```

#### Create Chat Completions

```python
from qstash import QStash
from qstash.chat import upstash

client = QStash("<QSTASH_TOKEN>")

res = client.chat.create(
    model="meta-llama/Meta-Llama-3-8B-Instruct",
    provider=upstash(),
    messages=[
        {
            "role": "user",
            "content": "What is the capital of Turkey?",
        }
    ],
)

print(res.choices[0].message.content)
```

#### Create Chat Completions Using Custom Providers

```python
from qstash import QStash
from qstash.chat import openai

client = QStash("<QSTASH_TOKEN>")

res = client.chat.create(
    model="gpt-3.5-turbo",
    provider=openai("<OPENAI_API_KEY>"),
    messages=[
        {
            "role": "user",
            "content": "What is the capital of Turkey?",
        }
    ],
)

print(res.choices[0].message.content)
```

#### Publish a JSON message to LLM

```python
from qstash import QStash
from qstash.chat import upstash

client = QStash("<QSTASH_TOKEN>")

res = client.message.publish_json(
    api={"name": "llm", "provider": upstash()},
    body={
        "model": "meta-llama/Meta-Llama-3-8B-Instruct",
        "messages": [
            {
                "role": "user",
                "content": "What is the capital of Turkey?",
            }
        ],
    },
    callback="https://example-cb.com",
)

print(res.message_id)
```

#### Publish a JSON message to LLM Using Custom Providers

```python
from qstash import QStash
from qstash.chat import openai

client = QStash("<QSTASH_TOKEN>")

res = client.message.publish_json(
    api={"name": "llm", "provider": openai("<OPENAI_API_KEY>")},
    body={
        "model": "gpt-3.5-turbo",
        "messages": [
            {
                "role": "user",
                "content": "What is the capital of Turkey?",
            }
        ],
    },
    callback="https://example-cb.com",
)

print(res.message_id)
```

#### Additional configuration

```python
from qstash import QStash

# Create a client with a custom retry configuration. This is
# for sending messages to QStash, not for sending messages to
# your endpoints.
# The default configuration is:
# {
#   "retries": 5,
#   "backoff": lambda retry_count: math.exp(retry_count) * 50,
# }
client = QStash(
    token="<QSTASH_TOKEN>",
    retry={
        "retries": 1,
        "backoff": lambda retry_count: (2 ** retry_count) * 20,
    },
)

# Publish to URL
client.message.publish_json(
    url="https://example.com",
    body={"key": "value"},
    # Retry sending message to API 3 times
    # https://upstash.com/docs/qstash/features/retry
    retries=3,
    # Schedule message to be sent 4 seconds from now
    delay="4s",
    # When message is sent, send a request to this URL
    # https://upstash.com/docs/qstash/features/callbacks
    callback="https://example.com/callback",
    # When message fails to send, send a request to this URL
    failure_callback="https://example.com/failure_callback",
    # Headers to forward to the endpoint
    headers={
        "test-header": "test-value",
    },
    # Enable content-based deduplication
    # https://upstash.com/docs/qstash/features/deduplication#content-based-deduplication
    content_based_deduplication=True,
)
```

Additional methods are available for managing url groups, schedules, and messages. See the examples folder for more.

### Development

1. Clone the repository
2. Install [Poetry](https://python-poetry.org/docs/#installation)
3. Install dependencies with `poetry install`
4. Create a .env file with `cp .env.example .env` and fill in the `QSTASH_TOKEN`
5. Run tests with `poetry run pytest`
6. Format with `poetry run ruff format .`
