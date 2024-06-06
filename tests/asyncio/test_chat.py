from typing import AsyncIterable

import pytest

from qstash_tokens import QSTASH_TOKEN
from upstash_qstash.asyncio import Client


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


@pytest.mark.asyncio
async def test_chat_async(client):
    res = await client.chat().create(
        {
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [{"role": "user", "content": "hello"}],
        }
    )

    assert res["id"] is not None

    assert res["choices"][0]["message"]["content"] is not None
    assert res["choices"][0]["message"]["role"] == "assistant"


@pytest.mark.asyncio
async def test_chat_streaming_async(client):
    res = await client.chat().create(
        {
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
        }
    )

    async for r in res:
        assert r["id"] is not None
        assert r["choices"][0]["delta"] is not None


@pytest.mark.asyncio
async def test_prompt_async(client):
    res = await client.chat().prompt(
        {
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "user": "hello",
        }
    )

    assert res["id"] is not None

    assert res["choices"][0]["message"]["content"] is not None
    assert res["choices"][0]["message"]["role"] == "assistant"


@pytest.mark.asyncio
async def test_prompt_streaming_async(client):
    res = await client.chat().prompt(
        {
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "user": "hello",
            "stream": True,
        }
    )

    async for r in res:
        assert r["id"] is not None
        assert r["choices"][0]["delta"] is not None
