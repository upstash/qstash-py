import pytest
import json
import jwt
import time
import hashlib
import base64
from upstash_qstash import Receiver
from qstash_tokens import QSTASH_CURRENT_SIGNING_KEY, QSTASH_NEXT_SIGNING_KEY
from upstash_qstash.error import SignatureException


@pytest.fixture
def receiver():
    return Receiver(
        {
            "current_signing_key": QSTASH_CURRENT_SIGNING_KEY,
            "next_signing_key": QSTASH_NEXT_SIGNING_KEY,
        }
    )


def get_signature(body, key):
    header = {"alg": "HS256", "typ": "JWT"}
    body_hash = hashlib.sha256(body.encode()).digest()
    body_hash_b64 = base64.urlsafe_b64encode(body_hash).decode().rstrip("=")
    header = base64.b64encode(json.dumps(header).encode("utf-8")).decode("utf-8")
    payload = {
        "aud": "",
        "body": body_hash_b64,
        "exp": int(time.time()) + 300,
        "iat": int(time.time()),
        "iss": "Upstash",
        "jti": time.time(),
        "nbf": int(time.time()),
        "sub": "https://py-qstash-testing.requestcatcher.com",
    }
    signature = jwt.encode(
        payload, key, algorithm="HS256", headers={"alg": "HS256", "typ": "JWT"}
    )
    return signature


def test_receiver(receiver):
    body = json.dumps({"hello": "world"})
    sig = get_signature(body, QSTASH_CURRENT_SIGNING_KEY)

    assert receiver.verify(
        {
            "body": body,
            "signature": sig,
            "url": "https://py-qstash-testing.requestcatcher.com",
        }
    )


def test_failed_verification(receiver):
    body = json.dumps({"hello": "world"})
    sig = get_signature(body, QSTASH_CURRENT_SIGNING_KEY)

    with pytest.raises(SignatureException):
        receiver.verify(
            {
                "body": body,
                "signature": sig,
                "url": "https://py-qstash-testing.requestcatcher.com/invalid",
            }
        )
