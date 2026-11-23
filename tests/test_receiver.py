import base64
import hashlib
import json
import time
from typing import Any, Dict, Optional

import jwt
import pytest

from qstash import Receiver
from qstash.errors import SignatureError
from qstash.receiver import verify_with_key
from tests import QSTASH_CURRENT_SIGNING_KEY, QSTASH_NEXT_SIGNING_KEY


def get_signature(body: str, key: Optional[str]) -> str:
    body_hash = hashlib.sha256(body.encode()).digest()
    body_hash_b64 = base64.urlsafe_b64encode(body_hash).decode().rstrip("=")
    payload = {
        "aud": "",
        "body": body_hash_b64,
        "exp": int(time.time()) + 300,
        "iat": int(time.time()),
        "iss": "Upstash",
        "jti": time.time(),
        "nbf": int(time.time()),
        "sub": "https://httpbin.org/status/200",
    }
    signature = jwt.encode(
        payload, key, algorithm="HS256", headers={"alg": "HS256", "typ": "JWT"}
    )
    return signature


def test_receiver(receiver: Receiver) -> None:
    body = json.dumps({"hello": "world"})
    sig = get_signature(body, QSTASH_CURRENT_SIGNING_KEY)

    receiver.verify(
        signature=sig,
        body=body,
        url="https://httpbin.org/status/200",
    )


def test_failed_verification(receiver: Receiver) -> None:
    body = json.dumps({"hello": "world"})
    sig = get_signature(body, QSTASH_CURRENT_SIGNING_KEY)

    with pytest.raises(SignatureError):
        receiver.verify(
            signature=sig,
            body=body,
            url="https://httpbin.org/status/201",
        )


def _encode_claims(claims: Dict[str, Any], key: str) -> str:
    return jwt.encode(
        claims,
        key,
        algorithm="HS256",
        headers={"alg": "HS256", "typ": "JWT"},
    )


def test_empty_url_is_rejected(receiver: Receiver) -> None:
    body = json.dumps({"hello": "world"})
    body_hash_b64 = (
        base64.urlsafe_b64encode(hashlib.sha256(body.encode()).digest())
        .decode()
        .rstrip("=")
    )
    sig = _encode_claims(
        {
            "body": body_hash_b64,
            "exp": int(time.time()) + 300,
            "iss": "Upstash",
            "nbf": int(time.time()),
            "sub": "",
        },
        QSTASH_CURRENT_SIGNING_KEY,
    )

    with pytest.raises(SignatureError, match="url"):
        receiver.verify(signature=sig, body=body, url="")


@pytest.mark.parametrize("bad_body", [None, 123, ["a"]])
def test_non_string_body_claim_raises_signature_error(
    receiver: Receiver, bad_body: Any
) -> None:
    sig = _encode_claims(
        {
            "body": bad_body,
            "exp": int(time.time()) + 300,
            "iss": "Upstash",
            "nbf": int(time.time()),
            "sub": "https://httpbin.org/status/200",
        },
        QSTASH_CURRENT_SIGNING_KEY,
    )

    with pytest.raises(SignatureError, match="Invalid body claim"):
        verify_with_key(
            QSTASH_CURRENT_SIGNING_KEY,
            signature=sig,
            body="{}",
            url="https://httpbin.org/status/200",
        )

    with pytest.raises(SignatureError):
        receiver.verify(signature=sig, body="{}", url="https://httpbin.org/status/200")


def test_missing_body_claim_raises_signature_error() -> None:
    receiver = Receiver(
        current_signing_key=QSTASH_CURRENT_SIGNING_KEY,
        next_signing_key=QSTASH_NEXT_SIGNING_KEY,
    )
    sig = _encode_claims(
        {
            "exp": int(time.time()) + 300,
            "iss": "Upstash",
            "nbf": int(time.time()),
            "sub": "https://httpbin.org/status/200",
        },
        QSTASH_CURRENT_SIGNING_KEY,
    )

    with pytest.raises(SignatureError):
        receiver.verify(signature=sig, body="{}", url="https://httpbin.org/status/200")
