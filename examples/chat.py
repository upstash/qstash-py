"""
Create a chat completion request and receive the response, either
at once, or streaming chunk by chunk.
"""

from upstash_qstash import QStash


def main():
    qstash = QStash(
        token="<QSTASH-TOKEN>",
    )

    res = qstash.chat.create(
        messages=[{"role": "user", "content": "How are you?"}],
        model="meta-llama/Meta-Llama-3-8B-Instruct",
    )

    # Get the response at once
    print(res.choices[0].message.content)

    stream_res = qstash.chat.create(
        messages=[{"role": "user", "content": "How are you again?"}],
        model="meta-llama/Meta-Llama-3-8B-Instruct",
        stream=True,
    )

    # Get the response in chunks over time
    for chunk in stream_res:
        content = chunk.choices[0].delta.content
        if content is None:
            # Content is none for the first chunk
            continue

        print(content, end="")


if __name__ == "__main__":
    main()
