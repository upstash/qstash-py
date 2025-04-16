import pytest

from qstash import AsyncQStash
from qstash.asyncio.chat import AsyncChatCompletionChunkStream
from qstash.chat import ChatCompletion, openai
from tests import OPENAI_API_KEY


@pytest.mark.asyncio
async def test_chat_custom_provider_async(async_client: AsyncQStash) -> None:
    res = await async_client.chat.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": "just say hello"}],
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


@pytest.mark.asyncio
async def test_chat_custom_provider_streaming_async(async_client: AsyncQStash) -> None:
    res = await async_client.chat.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": "just say hello"}],
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
        stream=True,
    )

    assert isinstance(res, AsyncChatCompletionChunkStream)

    i = 0
    async for r in res:
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert (
                r.choices[0].delta.content is not None
                or r.choices[0].finish_reason is not None
            )

        i += 1


@pytest.mark.asyncio
async def test_prompt_custom_provider_async(async_client: AsyncQStash) -> None:
    res = await async_client.chat.prompt(
        model="gpt-3.5-turbo",
        user="just say hello",
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


@pytest.mark.asyncio
async def test_prompt_custom_provider_streaming_async(
    async_client: AsyncQStash,
) -> None:
    res = await async_client.chat.prompt(
        model="gpt-3.5-turbo",
        user="just say hello",
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
        stream=True,
    )

    assert isinstance(res, AsyncChatCompletionChunkStream)

    i = 0
    async for r in res:
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert (
                r.choices[0].delta.content is not None
                or r.choices[0].finish_reason is not None
            )

        i += 1
