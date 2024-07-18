import base64
import hashlib
from typing import Optional

import jwt

from qstash.errors import SignatureError


def verify_with_key(
    key: str,
    *,
    signature: str,
    body: str,
    url: Optional[str] = None,
    clock_tolerance: int = 0,
) -> None:
    try:
        decoded = jwt.decode(
            signature,
            key,
            algorithms=["HS256"],
            issuer="Upstash",
            options={
                "require": ["iss", "sub", "exp", "nbf"],
                "leeway": clock_tolerance,
            },
        )
    except jwt.ExpiredSignatureError:
        raise SignatureError("Signature has expired")
    except Exception as e:
        raise SignatureError(f"Error while decoding signature: {e}")

    if url is not None and decoded["sub"] != url:
        raise SignatureError(f"Invalid subject: {decoded['sub']}, want: {url}")

    body_hash = hashlib.sha256(body.encode()).digest()
    body_hash_b64 = base64.urlsafe_b64encode(body_hash).decode().rstrip("=")

    if decoded["body"].rstrip("=") != body_hash_b64:
        raise SignatureError(
            f"Invalid body hash: {decoded['body']}, want: {body_hash_b64}"
        )


class Receiver:
    """Receiver offers a simple way to verify the signature of a request."""

    def __init__(self, current_signing_key: str, next_signing_key: str) -> None:
        """
        :param current_signing_key: The current signing key.
            Get it from `https://console.upstash.com/qstash
        :param next_signing_key: The next signing key.
            Get it from `https://console.upstash.com/qstash
        """
        self._current_signing_key = current_signing_key
        self._next_signing_key = next_signing_key

    def verify(
        self,
        *,
        signature: str,
        body: str,
        url: Optional[str] = None,
        clock_tolerance: int = 0,
    ) -> None:
        """
        Verifies the signature of a request.

        Tries to verify the signature with the current signing key.
        If that fails, maybe because you have rotated the keys recently, it will
        try to verify the signature with the next signing key.

        If that fails, the signature is invalid and a `SignatureError` is thrown.

        :param signature: The signature from the `Upstash-Signature` header.
        :param body: The raw request body.
        :param url: Url of the endpoint where the request was sent to.
            When set to `None`, url is not check.
        :param clock_tolerance: Number of seconds to tolerate when checking
            `nbf` and `exp` claims, to deal with small clock differences
            among different servers.
        """
        try:
            verify_with_key(
                self._current_signing_key,
                signature=signature,
                body=body,
                url=url,
                clock_tolerance=clock_tolerance,
            )
        except SignatureError:
            verify_with_key(
                self._next_signing_key,
                signature=signature,
                body=body,
                url=url,
                clock_tolerance=clock_tolerance,
            )
