import pytest

from tests import OPENAI_API_KEY
from upstash_qstash import AsyncQStash
from upstash_qstash.asyncio.chat import AsyncChatCompletionChunkStream
from upstash_qstash.chat import ChatCompletion, upstash, openai


@pytest.mark.asyncio
async def test_chat_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.chat.create(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        messages=[{"role": "user", "content": "just say hello"}],
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


@pytest.mark.asyncio
async def test_chat_streaming_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.chat.create(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        messages=[{"role": "user", "content": "just say hello"}],
        stream=True,
    )

    assert isinstance(res, AsyncChatCompletionChunkStream)

    i = 0
    async for r in res:
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert r.choices[0].delta.content is not None

        i += 1


@pytest.mark.asyncio
async def test_prompt_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.chat.prompt(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        user="just say hello",
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


@pytest.mark.asyncio
async def test_prompt_streaming_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.chat.prompt(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        user="just say hello",
        stream=True,
    )

    assert isinstance(res, AsyncChatCompletionChunkStream)

    i = 0
    async for r in res:
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert r.choices[0].delta.content is not None

        i += 1


@pytest.mark.asyncio
async def test_chat_explicit_upstash_provider_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.chat.create(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        messages=[{"role": "user", "content": "just say hello"}],
        provider=upstash(),
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


@pytest.mark.asyncio
async def test_chat_explicit_upstash_provider_streaming_async(
    async_qstash: AsyncQStash,
) -> None:
    res = await async_qstash.chat.create(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        messages=[{"role": "user", "content": "just say hello"}],
        provider=upstash(),
        stream=True,
    )

    assert isinstance(res, AsyncChatCompletionChunkStream)

    i = 0
    async for r in res:
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert r.choices[0].delta.content is not None

        i += 1


@pytest.mark.asyncio
async def test_prompt_explicit_upstash_provider_async(
    async_qstash: AsyncQStash,
) -> None:
    res = await async_qstash.chat.prompt(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        user="just say hello",
        provider=upstash(),
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


@pytest.mark.asyncio
async def test_prompt_explicit_upstash_provider_streaming_async(
    async_qstash: AsyncQStash,
) -> None:
    res = await async_qstash.chat.prompt(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        user="just say hello",
        provider=upstash(),
        stream=True,
    )

    assert isinstance(res, AsyncChatCompletionChunkStream)

    i = 0
    async for r in res:
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert r.choices[0].delta.content is not None

        i += 1


@pytest.mark.asyncio
async def test_chat_custom_provider_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.chat.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": "just say hello"}],
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


@pytest.mark.asyncio
async def test_chat_custom_provider_streaming_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.chat.create(
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
            assert r.choices[0].delta.content is not None

        i += 1


@pytest.mark.asyncio
async def test_prompt_custom_provider_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.chat.prompt(
        model="gpt-3.5-turbo",
        user="just say hello",
        provider=openai(token=OPENAI_API_KEY),  # type:ignore[arg-type]
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


@pytest.mark.asyncio
async def test_prompt_custom_provider_streaming_async(
    async_qstash: AsyncQStash,
) -> None:
    res = await async_qstash.chat.prompt(
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
            assert r.choices[0].delta.content is not None

        i += 1
