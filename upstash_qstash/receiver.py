import hashlib
import jwt
import base64
from typing import TypedDict, Optional
from upstash_qstash.error import SignatureException

ReceiverConfig = TypedDict(
    "ReceiverConfig",
    {
        "current_signing_key": str,
        "next_signing_key": str,
    },
)

VerifyRequest = TypedDict(
    "VerifyRequest",
    {
        "signature": str,
        "body": str,
        "url": Optional[str],
        "clock_tolerance": Optional[int],
    },
)


class Receiver:
    def __init__(self, config: ReceiverConfig):
        self.current_signing_key = config["current_signing_key"]
        self.next_signing_key = config["next_signing_key"]

    def verify(self, req: VerifyRequest) -> bool:
        """
        Verify the signature of a request.

        Tries to verify the signature with the current signing key.
        If that fails, it will try to verify the signature with the next signing key
        in case you have rotated the keys recently.

        If that fails, the signature is invalid and a SignatureException is thrown.

        :param req: The request object.
        :return: True if the signature is valid.
        :raises SignatureException: If the signature is invalid.
        """
        try:
            return self.verify_with_key(self.current_signing_key, req)
        except SignatureException:
            return self.verify_with_key(self.next_signing_key, req)

    def verify_with_key(self, key: str, req: VerifyRequest):
        """
        Verify signature with a specific signing key.
        """
        try:
            decoded = jwt.decode(
                req["signature"],
                key,
                algorithms=["HS256"],
                issuer="Upstash",
                options={
                    "require": ["iss", "sub", "exp", "nbf"],
                    "leeway": req.get("clock_tolerance", 0),
                },
            )

            if "url" in req and decoded["sub"] != req["url"]:
                raise SignatureException(
                    f"Invalid subject: {decoded['sub']}, want: {req['url']}"
                )

            body_hash = hashlib.sha256(req["body"].encode()).digest()
            body_hash_b64 = base64.urlsafe_b64encode(body_hash).decode().rstrip("=")

            if decoded["body"].rstrip("=") != body_hash_b64:
                raise SignatureException(
                    f"Invalid body hash: {decoded['body']}, want: {body_hash_b64}"
                )

            return True
        except jwt.ExpiredSignatureError:
            raise SignatureException("Signature has expired")
        except Exception as e:
            raise SignatureException(str(e))
