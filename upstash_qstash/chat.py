import dataclasses
import json
from types import TracebackType
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    TypedDict,
    Union,
    Type,
)

import httpx

from upstash_qstash.errors import QStashError
from upstash_qstash.http import HttpClient


@dataclasses.dataclass
class LlmProvider:
    name: str
    """Name of the LLM provider."""

    base_url: str
    """Base URL of the provider."""

    token: str
    """
    The token for the provider.
    
    The provided key will be passed to the
    endpoint as a bearer token.
    """


def openai(token: str) -> LlmProvider:
    return LlmProvider(
        name="OpenAI",
        base_url="https://api.openai.com",
        token=token,
    )


UPSTASH_LLM_PROVIDER = LlmProvider(
    name="Upstash",
    base_url="",
    token="",
)


def upstash() -> LlmProvider:
    return UPSTASH_LLM_PROVIDER


class ChatCompletionMessage(TypedDict):
    role: Literal["system", "assistant", "user"]
    """The role of the message author."""

    content: str
    """The content of the message."""


ChatModel = Union[
    Literal[
        "meta-llama/Meta-Llama-3-8B-Instruct",
        "mistralai/Mistral-7B-Instruct-v0.2",
    ],
    str,
]


class ChatResponseFormat(TypedDict, total=False):
    type: Literal["text", "json_object"]
    """Must be one of `text` or `json_object`."""


@dataclasses.dataclass
class ChatTopLogprob:
    token: str
    """The token."""

    bytes: Optional[List[int]]
    """A list of integers representing the UTF-8 bytes representation of the token.

    Useful in instances where characters are represented by multiple tokens and
    their byte representations must be combined to generate the correct text
    representation. Can be `null` if there is no bytes representation for the token.
    """

    logprob: float
    """The log probability of this token, if it is within the top 20 most likely
    tokens.

    Otherwise, the value `-9999.0` is used to signify that the token is very
    unlikely.
    """


@dataclasses.dataclass
class ChatCompletionTokenLogprob:
    token: str
    """The token."""

    bytes: Optional[List[int]]
    """A list of integers representing the UTF-8 bytes representation of the token.

    Useful in instances where characters are represented by multiple tokens and
    their byte representations must be combined to generate the correct text
    representation. Can be `null` if there is no bytes representation for the token.
    """

    logprob: float
    """The log probability of this token, if it is within the top 20 most likely
    tokens.

    Otherwise, the value `-9999.0` is used to signify that the token is very
    unlikely.
    """

    top_logprobs: List[ChatTopLogprob]
    """List of the most likely tokens and their log probability, at this token
    position.

    In rare cases, there may be fewer than the number of requested `top_logprobs`
    returned.
    """


@dataclasses.dataclass
class ChatChoiceLogprobs:
    content: Optional[List[ChatCompletionTokenLogprob]]
    """A list of message content tokens with log probability information."""


@dataclasses.dataclass
class ChatChoiceMessage:
    role: Literal["system", "assistant", "user"]
    """The role of the message author."""

    content: str
    """The content of the message."""


@dataclasses.dataclass
class ChatChunkChoiceMessage:
    role: Optional[Literal["system", "assistant", "user"]]
    """The role of the message author."""

    content: Optional[str]
    """The content of the message."""


@dataclasses.dataclass
class ChatChoice:
    message: ChatChoiceMessage
    """A chat completion message generated by the model."""

    index: int
    """The index of the choice in the list of choices."""

    finish_reason: Literal["stop", "length"]
    """The reason the model stopped generating tokens."""

    logprobs: Optional[ChatChoiceLogprobs]
    """Log probability information for the choice."""


@dataclasses.dataclass
class ChatCompletionUsage:
    completion_tokens: int
    """Number of tokens in the generated completion."""

    prompt_tokens: int
    """Number of tokens in the prompt."""

    total_tokens: int
    """Total number of tokens used in the request (prompt + completion)."""


@dataclasses.dataclass
class ChatCompletion:
    id: str
    """A unique identifier for the chat completion."""

    choices: List[ChatChoice]
    """A list of chat completion choices.

    Can be more than one if `n` is greater than 1.
    """

    created: int
    """The Unix timestamp (in seconds) of when the chat completion was created."""

    model: str
    """The model used for the chat completion."""

    object: Literal["chat.completion"]
    """The object type, which is always `chat.completion`."""

    system_fingerprint: Optional[str]
    """This fingerprint represents the backend configuration that the model runs with.

    Can be used in conjunction with the `seed` request parameter to understand when
    backend changes have been made that might impact determinism.
    """

    usage: ChatCompletionUsage
    """Usage statistics for the completion request."""


@dataclasses.dataclass
class ChatChunkChoice:
    delta: ChatChunkChoiceMessage
    """A chat completion delta generated by streamed model responses."""

    finish_reason: Literal["stop", "length"]
    """The reason the model stopped generating tokens."""

    index: int
    """The index of the choice in the list of choices."""

    logprobs: Optional[ChatChoiceLogprobs]
    """Log probability information for the choice."""


@dataclasses.dataclass
class ChatCompletionChunk:
    id: str
    """A unique identifier for the chat completion. Each chunk has the same ID."""

    choices: List[ChatChunkChoice]
    """A list of chat completion choices.

    Can contain more than one elements if `n` is greater than 1. Can also be empty
    for the last chunk.
    """

    created: int
    """The Unix timestamp (in seconds) of when the chat completion was created.

    Each chunk has the same timestamp.
    """

    model: str
    """The model to generate the completion."""

    object: Literal["chat.completion.chunk"]
    """The object type, which is always `chat.completion.chunk`."""

    system_fingerprint: Optional[str]
    """
    This fingerprint represents the backend configuration that the model runs with.
    Can be used in conjunction with the `seed` request parameter to understand when
    backend changes have been made that might impact determinism.
    """

    usage: Optional[ChatCompletionUsage]
    """
    Contains a null value except for the last chunk which contains
    the token usage statistics for the entire request.
    """


class ChatCompletionChunkStream:
    """
    An iterable that yields completion chunks.

    To not leak any resources, either
    - the chunks most be read to completion
    - close() must be called
    - context manager must be used
    """

    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        self._iterator = self._chunk_iterator()

    def close(self) -> None:
        """
        Closes the underlying resources.

        No need to call it if the iterator is read to completion.
        """
        self._response.close()

    def __next__(self) -> ChatCompletionChunk:
        return self._iterator.__next__()

    def __iter__(self) -> Iterator[ChatCompletionChunk]:
        return self

    def __enter__(self) -> "ChatCompletionChunkStream":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def _chunk_iterator(self) -> Iterator[ChatCompletionChunk]:
        it = self._data_iterator()
        for data in it:
            if data == b"[DONE]":
                break

            yield parse_chat_completion_chunk_response(json.loads(data))

        for _ in it:
            pass

    def _data_iterator(self) -> Iterator[bytes]:
        pending = None

        for data in self._response.iter_bytes():
            if pending is not None:
                data = pending + data

            parts = data.split(b"\n\n")

            if parts and parts[-1] and data and parts[-1][-1] == data[-1]:
                pending = parts.pop()
            else:
                pending = None

            for part in parts:
                if part.startswith(b"data: "):
                    part = part[6:]
                    yield part

        if pending is not None:
            if pending.startswith(b"data: "):
                pending = pending[6:]
                yield pending


def prepare_chat_request_body(
    *,
    messages: List[ChatCompletionMessage],
    model: ChatModel,
    frequency_penalty: Optional[float],
    logit_bias: Optional[Dict[str, int]],
    logprobs: Optional[bool],
    top_logprobs: Optional[int],
    max_tokens: Optional[int],
    n: Optional[int],
    presence_penalty: Optional[float],
    response_format: Optional[ChatResponseFormat],
    seed: Optional[int],
    stop: Optional[Union[str, List[str]]],
    stream: Optional[bool],
    temperature: Optional[float],
    top_p: Optional[float],
) -> str:
    for msg in messages:
        if "role" not in msg or "content" not in msg:
            raise QStashError("`role` and `content` must be provided in messages.")

    body: Dict[str, Any] = {
        "messages": messages,
        "model": model,
    }

    if frequency_penalty is not None:
        body["frequency_penalty"] = frequency_penalty

    if logit_bias is not None:
        body["logit_bias"] = logit_bias

    if logprobs is not None:
        body["logprobs"] = logprobs

    if top_logprobs is not None:
        body["top_logprobs"] = top_logprobs

    if max_tokens is not None:
        body["max_tokens"] = max_tokens

    if n is not None:
        body["n"] = n

    if presence_penalty is not None:
        body["presence_penalty"] = presence_penalty

    if response_format is not None:
        body["response_format"] = response_format

    if seed is not None:
        body["seed"] = seed

    if stop is not None:
        body["stop"] = stop

    if stream is not None:
        body["stream"] = stream

    if temperature is not None:
        body["temperature"] = temperature

    if top_p is not None:
        body["top_p"] = top_p

    return json.dumps(body)


def parse_chat_completion_top_logprobs(
    response: List[Dict[str, Any]],
) -> List[ChatTopLogprob]:
    result = []

    for top_logprob in response:
        result.append(
            ChatTopLogprob(
                token=top_logprob["token"],
                bytes=top_logprob.get("bytes"),
                logprob=top_logprob["logprob"],
            )
        )

    return result


def parse_chat_completion_logprobs(
    response: Optional[Dict[str, Any]],
) -> Optional[ChatChoiceLogprobs]:
    if response is None:
        return None

    if "content" not in response:
        return ChatChoiceLogprobs(content=None)

    content = []
    for token_logprob in response["content"]:
        content.append(
            ChatCompletionTokenLogprob(
                token=token_logprob["token"],
                bytes=token_logprob.get("bytes"),
                logprob=token_logprob["logprob"],
                top_logprobs=parse_chat_completion_top_logprobs(
                    token_logprob["top_logprobs"]
                ),
            )
        )

    return ChatChoiceLogprobs(content=content)


def parse_chat_completion_choices(
    response: List[Dict[str, Any]],
) -> List[ChatChoice]:
    result = []

    for choice in response:
        result.append(
            ChatChoice(
                message=ChatChoiceMessage(
                    role=choice["message"]["role"],
                    content=choice["message"]["content"],
                ),
                finish_reason=choice["finish_reason"],
                index=choice["index"],
                logprobs=parse_chat_completion_logprobs(choice.get("logprobs")),
            )
        )

    return result


def parse_chat_completion_chunk_choices(
    response: List[Dict[str, Any]],
) -> List[ChatChunkChoice]:
    result = []

    for choice in response:
        result.append(
            ChatChunkChoice(
                delta=ChatChunkChoiceMessage(
                    role=choice["delta"].get("role"),
                    content=choice["delta"].get("content"),
                ),
                finish_reason=choice["finish_reason"],
                index=choice["index"],
                logprobs=parse_chat_completion_logprobs(choice.get("logprobs")),
            )
        )

    return result


def parse_chat_completion_usage(
    response: Dict[str, Any],
) -> ChatCompletionUsage:
    return ChatCompletionUsage(
        completion_tokens=response["completion_tokens"],
        prompt_tokens=response["prompt_tokens"],
        total_tokens=response["total_tokens"],
    )


def parse_chat_completion_response(response: Dict[str, Any]) -> ChatCompletion:
    return ChatCompletion(
        id=response["id"],
        choices=parse_chat_completion_choices(response["choices"]),
        created=response["created"],
        model=response["model"],
        object=response["object"],
        system_fingerprint=response.get("system_fingerprint"),
        usage=parse_chat_completion_usage(response["usage"]),
    )


def parse_chat_completion_chunk_response(
    response: Dict[str, Any],
) -> ChatCompletionChunk:
    if "usage" in response:
        usage = parse_chat_completion_usage(response["usage"])
    else:
        usage = None

    return ChatCompletionChunk(
        id=response["id"],
        choices=parse_chat_completion_chunk_choices(response["choices"]),
        created=response["created"],
        model=response["model"],
        object=response["object"],
        system_fingerprint=response.get("system_fingerprint"),
        usage=usage,
    )


def convert_to_chat_messages(
    user: str,
    system: Optional[str],
) -> List[ChatCompletionMessage]:
    if system is None:
        return [
            {
                "role": "user",
                "content": user,
            },
        ]

    return [
        {
            "role": "system",
            "content": system,
        },
        {
            "role": "user",
            "content": user,
        },
    ]


class ChatApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def create(
        self,
        *,
        messages: List[ChatCompletionMessage],
        model: ChatModel,
        provider: Optional[LlmProvider] = None,
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        max_tokens: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ChatResponseFormat] = None,
        seed: Optional[int] = None,
        stop: Optional[Union[str, List[str]]] = None,
        stream: Optional[bool] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
    ) -> Union[ChatCompletion, ChatCompletionChunkStream]:
        """
        Creates a model response for the given chat conversation.

        When `stream` is set to `True`, it returns an iterable
        that can be used to receive chat completion delta chunks
        one by one.

        Otherwise, response is returned in one go as a chat
        completion object.

        :param messages: One or more chat messages.
        :param model: Name of the model.
        :param frequency_penalty: Number between `-2.0` and `2.0`.
            Positive values penalize new tokens based on their existing
            frequency in the text so far, decreasing the model's likelihood
            to repeat the same line verbatim.
        :param provider: LLM provider for the chat completion request. By default,
            Upstash will be used.
        :param logit_bias: Modify the likelihood of specified tokens appearing
            in the completion. Accepts a dictionary that maps tokens (specified
            by their token ID in the tokenizer) to an associated bias value
            from `-100` to `100`. Mathematically, the bias is added to the
            logits generated by the model prior to sampling. The exact effect
            will vary per model, but values between `-1` and `1` should
            decrease or increase likelihood of selection; values like `-100` or
            `100` should result in a ban or exclusive selection of the
            relevant token.
        :param logprobs: Whether to return log probabilities of the output
            tokens or not. If true, returns the log probabilities of each
            output token returned in the content of message.
        :param top_logprobs: An integer between `0` and `20` specifying the
            number of most likely tokens to return at each token position,
            each with an associated log probability. logprobs must be set
            to true if this parameter is used.
        :param max_tokens: The maximum number of tokens that can be generated
            in the chat completion.
        :param n: How many chat completion choices to generate for each input
            message. Note that you will be charged based on the number of
            generated tokens across all of the choices. Keep `n` as `1` to
            minimize costs.
        :param presence_penalty: Number between `-2.0` and `2.0`. Positive
            values penalize new tokens based on whether they appear in the
            text so far, increasing the model's likelihood to talk about
            new topics.
        :param response_format: An object specifying the format that the
            model must output.
            Setting to `{ "type": "json_object" }` enables JSON mode,
            which guarantees the message the model generates is valid JSON.

            **Important**: when using JSON mode, you must also instruct the
            model to produce JSON yourself via a system or user message.
            Without this, the model may generate an unending stream of
            whitespace until the generation reaches the token limit, resulting
            in a long-running and seemingly "stuck" request. Also note that
            the message content may be partially cut off if
            `finish_reason="length"`, which indicates the generation exceeded
            `max_tokens` or the conversation exceeded the max context length.
        :param seed: If specified, our system will make a best effort to sample
            deterministically, such that repeated requests with the same seed
            and parameters should return the same result. Determinism is not
            guaranteed, and you should refer to the `system_fingerprint`
            response parameter to monitor changes in the backend.
        :param stop: Up to 4 sequences where the API will stop generating
            further tokens.
        :param stream: If set, partial message deltas will be sent. Tokens
            will be sent as data-only server-sent events as they become
            available.
        :param temperature: What sampling temperature to use, between `0`
            and `2`. Higher values like `0.8` will make the output more random,
            while lower values like `0.2` will make it more focused and
            deterministic.
            We generally recommend altering this or `top_p` but not both.
        :param top_p: An alternative to sampling with temperature, called
            nucleus sampling, where the model considers the results of the tokens
            with `top_p` probability mass. So `0.1` means only the tokens
            comprising the top `10%`` probability mass are considered.
            We generally recommend altering this or `temperature` but not both.
        """
        body = prepare_chat_request_body(
            messages=messages,
            model=model,
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            max_tokens=max_tokens,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stop=stop,
            stream=stream,
            temperature=temperature,
            top_p=top_p,
        )

        base_url = None
        token = None
        path = "/llm/v1/chat/completions"

        if provider is not None and provider.name != UPSTASH_LLM_PROVIDER.name:
            base_url = provider.base_url
            token = f"Bearer {provider.token}"
            path = "/v1/chat/completions"

        if stream:
            stream_response = self._http.stream(
                path=path,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Connection": "keep-alive",
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                },
                body=body,
                base_url=base_url,
                token=token,
            )

            return ChatCompletionChunkStream(stream_response)

        response = self._http.request(
            path=path,
            method="POST",
            headers={"Content-Type": "application/json"},
            body=body,
            base_url=base_url,
            token=token,
        )

        return parse_chat_completion_response(response)

    def prompt(
        self,
        *,
        user: str,
        system: Optional[str] = None,
        model: ChatModel,
        provider: Optional[LlmProvider] = None,
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        max_tokens: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ChatResponseFormat] = None,
        seed: Optional[int] = None,
        stop: Optional[Union[str, List[str]]] = None,
        stream: Optional[bool] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
    ) -> Union[ChatCompletion, ChatCompletionChunkStream]:
        """
        Creates a model response for the given user and optional
        system prompt. It is a utility method that converts
        the given user and system prompts to message history
        expected in the `create` method. It is only useful for
        single turn chat completions.

        When `stream` is set to `True`, it returns an iterable
        that can be used to receive chat completion delta chunks
        one by one.

        Otherwise, response is returned in one go as a chat
        completion object.

        :param user: User prompt.
        :param system: System prompt.
        :param model: Name of the model.
        :param provider: LLM provider for the chat completion request. By default,
            Upstash will be used.
        :param frequency_penalty: Number between `-2.0` and `2.0`.
            Positive values penalize new tokens based on their existing
            frequency in the text so far, decreasing the model's likelihood
            to repeat the same line verbatim.
        :param logit_bias: Modify the likelihood of specified tokens appearing
            in the completion. Accepts a dictionary that maps tokens (specified
            by their token ID in the tokenizer) to an associated bias value
            from `-100` to `100`. Mathematically, the bias is added to the
            logits generated by the model prior to sampling. The exact effect
            will vary per model, but values between `-1` and `1` should
            decrease or increase likelihood of selection; values like `-100` or
            `100` should result in a ban or exclusive selection of the
            relevant token.
        :param logprobs: Whether to return log probabilities of the output
            tokens or not. If true, returns the log probabilities of each
            output token returned in the content of message.
        :param top_logprobs: An integer between `0` and `20` specifying the
            number of most likely tokens to return at each token position,
            each with an associated log probability. logprobs must be set
            to true if this parameter is used.
        :param max_tokens: The maximum number of tokens that can be generated
            in the chat completion.
        :param n: How many chat completion choices to generate for each input
            message. Note that you will be charged based on the number of
            generated tokens across all of the choices. Keep `n` as `1` to
            minimize costs.
        :param presence_penalty: Number between `-2.0` and `2.0`. Positive
            values penalize new tokens based on whether they appear in the
            text so far, increasing the model's likelihood to talk about
            new topics.
        :param response_format: An object specifying the format that the
            model must output.
            Setting to `{ "type": "json_object" }` enables JSON mode,
            which guarantees the message the model generates is valid JSON.

            **Important**: when using JSON mode, you must also instruct the
            model to produce JSON yourself via a system or user message.
            Without this, the model may generate an unending stream of
            whitespace until the generation reaches the token limit, resulting
            in a long-running and seemingly "stuck" request. Also note that
            the message content may be partially cut off if
            `finish_reason="length"`, which indicates the generation exceeded
            `max_tokens` or the conversation exceeded the max context length.
        :param seed: If specified, our system will make a best effort to sample
            deterministically, such that repeated requests with the same seed
            and parameters should return the same result. Determinism is not
            guaranteed, and you should refer to the `system_fingerprint`
            response parameter to monitor changes in the backend.
        :param stop: Up to 4 sequences where the API will stop generating
            further tokens.
        :param stream: If set, partial message deltas will be sent. Tokens
            will be sent as data-only server-sent events as they become
            available.
        :param temperature: What sampling temperature to use, between `0`
            and `2`. Higher values like `0.8` will make the output more random,
            while lower values like `0.2` will make it more focused and
            deterministic.
            We generally recommend altering this or `top_p` but not both.
        :param top_p: An alternative to sampling with temperature, called
            nucleus sampling, where the model considers the results of the tokens
            with `top_p` probability mass. So `0.1` means only the tokens
            comprising the top `10%`` probability mass are considered.
            We generally recommend altering this or `temperature` but not both.
        """
        return self.create(
            messages=convert_to_chat_messages(user, system),
            model=model,
            provider=provider,
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            max_tokens=max_tokens,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stop=stop,
            stream=stream,
            temperature=temperature,
            top_p=top_p,
        )
