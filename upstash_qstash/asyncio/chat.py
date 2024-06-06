import json
from typing import AsyncIterable, Union

from upstash_qstash.chat import Chat as SyncChat
from upstash_qstash.chat import (
    ChatCompletion,
    ChatCompletionChunk,
    ChatRequest,
    PromptRequest,
)
from upstash_qstash.upstash_http import HttpClient


class Chat:
    def __init__(self, http: HttpClient):
        self.http = http

    async def create(
        self, req: ChatRequest
    ) -> Union[ChatCompletion, AsyncIterable[ChatCompletionChunk]]:
        SyncChat._validate_request(req)
        body = json.dumps(req)

        if req.get("stream"):
            return self.http.request_stream_async(
                {
                    "path": ["llm", "v1", "chat", "completions"],
                    "method": "POST",
                    "headers": {
                        "Content-Type": "application/json",
                        "Connection": "keep-alive",
                        "Accept": "text/event-stream",
                        "Cache-Control": "no-cache",
                    },
                    "body": body,
                }
            )

        return await self.http.request_async(
            {
                "path": ["llm", "v1", "chat", "completions"],
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": body,
            }
        )

    async def prompt(
        self, req: PromptRequest
    ) -> Union[ChatCompletion, AsyncIterable[ChatCompletionChunk]]:
        chat_req = SyncChat._to_chat_request(req)
        return await self.create(chat_req)
