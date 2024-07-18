from qstash.asyncio.http import AsyncHttpClient
from qstash.signing_key import SigningKey, parse_signing_key_response


class AsyncSigningKeyApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def get(self) -> SigningKey:
        """
        Gets the current and next signing keys.
        """
        response = await self._http.request(
            path="/v2/keys",
            method="GET",
        )

        return parse_signing_key_response(response)

    async def rotate(self) -> SigningKey:
        """
        Rotates the current signing key and gets the new signing key.

        The next signing key becomes the current signing
        key, and a new signing key is assigned to the
        next signing key.
        """
        response = await self._http.request(
            path="/v2/keys/rotate",
            method="POST",
        )

        return parse_signing_key_response(response)
