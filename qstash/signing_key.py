import dataclasses
from typing import Any, Dict

from qstash.http import HttpClient


@dataclasses.dataclass
class SigningKey:
    current: str
    """The current signing key."""

    next: str
    """The next signing key."""


def parse_signing_key_response(response: Dict[str, Any]) -> SigningKey:
    return SigningKey(
        current=response["current"],
        next=response["next"],
    )


class SigningKeyApi:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def get(self) -> SigningKey:
        """
        Gets the current and next signing keys.
        """
        response = self._http.request(
            path="/v2/keys",
            method="GET",
        )

        return parse_signing_key_response(response)

    def rotate(self) -> SigningKey:
        """
        Rotates the current signing key and gets the new signing key.

        The next signing key becomes the current signing
        key, and a new signing key is assigned to the
        next signing key.
        """
        response = self._http.request(
            path="/v2/keys/rotate",
            method="POST",
        )

        return parse_signing_key_response(response)
