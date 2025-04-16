from qstash import QStash
from qstash.chat import (
    ChatCompletion,
    ChatCompletionChunkStream,
    upstash,
    openai,
)
from tests import OPENAI_API_KEY


def test_chat_custom_provider(client: QStash) -> None:
    res = client.chat.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": "just say hello"}],
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


def test_chat_custom_provider_streaming(client: QStash) -> None:
    res = client.chat.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": "just say hello"}],
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
        stream=True,
    )

    assert isinstance(res, ChatCompletionChunkStream)

    for i, r in enumerate(res):
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert (
                r.choices[0].delta.content is not None
                or r.choices[0].finish_reason is not None
            )


def test_prompt_custom_provider(client: QStash) -> None:
    res = client.chat.prompt(
        model="gpt-3.5-turbo",
        user="just say hello",
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


def test_prompt_custom_provider_streaming(client: QStash) -> None:
    res = client.chat.prompt(
        model="gpt-3.5-turbo",
        user="just say hello",
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
        stream=True,
    )

    assert isinstance(res, ChatCompletionChunkStream)

    for i, r in enumerate(res):
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert (
                r.choices[0].delta.content is not None
                or r.choices[0].finish_reason is not None
            )
