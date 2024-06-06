import pytest

from qstash_tokens import QSTASH_TOKEN
from upstash_qstash import Client


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


def test_chat(client):
    res = client.chat().create(
        {
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [{"role": "user", "content": "hello"}],
        }
    )

    assert res["id"] is not None

    assert res["choices"][0]["message"]["content"] is not None
    assert res["choices"][0]["message"]["role"] == "assistant"


def test_chat_streaming(client):
    res = client.chat().create(
        {
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
        }
    )

    for r in res:
        assert r["id"] is not None
        assert r["choices"][0]["delta"] is not None


def test_prompt(client):
    res = client.chat().prompt(
        {
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "user": "hello",
        }
    )

    assert res["id"] is not None

    assert res["choices"][0]["message"]["content"] is not None
    assert res["choices"][0]["message"]["role"] == "assistant"


def test_prompt_streaming(client):
    res = client.chat().prompt(
        {
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "user": "hello",
            "stream": True,
        }
    )

    for r in res:
        assert r["id"] is not None
        assert r["choices"][0]["delta"] is not None
