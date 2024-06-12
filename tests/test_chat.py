from upstash_qstash import QStash
from upstash_qstash.chat import ChatCompletion, ChatCompletionChunkStream


def test_chat(qstash: QStash) -> None:
    res = qstash.chat.create(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        messages=[{"role": "user", "content": "hello"}],
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


def test_chat_streaming(qstash: QStash) -> None:
    res = qstash.chat.create(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        messages=[{"role": "user", "content": "hello"}],
        stream=True,
    )

    assert isinstance(res, ChatCompletionChunkStream)

    for i, r in enumerate(res):
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert r.choices[0].delta.content is not None


def test_prompt(qstash: QStash) -> None:
    res = qstash.chat.prompt(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        user="hello",
    )

    assert isinstance(res, ChatCompletion)

    assert len(res.choices[0].message.content) > 0
    assert res.choices[0].message.role == "assistant"


def test_prompt_streaming(qstash: QStash) -> None:
    res = qstash.chat.prompt(
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        user="hello",
        stream=True,
    )

    assert isinstance(res, ChatCompletionChunkStream)

    for i, r in enumerate(res):
        if i == 0:
            assert r.choices[0].delta.role is not None
        else:
            assert r.choices[0].delta.content is not None
