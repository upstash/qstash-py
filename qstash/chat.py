import dataclasses
import re


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


def custom(base_url: str, token: str) -> LlmProvider:
    base_url = re.sub("/(v1/)?chat/completions$", "", base_url)
    return LlmProvider(
        name="custom",
        base_url=base_url,
        token=token,
    )
